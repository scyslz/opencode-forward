package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	clusterForwardPath = "/_cluster/forward"
	clusterPingPath    = "/_cluster/ping"
	maxHop             = 3
	clusterHdrVisited  = "X-Cluster-Visited"
	clusterHdrHop      = "X-Cluster-Hop"
	clusterHdrToken    = "X-Cluster-Token"
	clusterHdrEgress   = "X-Cluster-Egress"
)

type clusterFrameHeader struct {
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Visited []string          `json:"visited"`
	Hop     int               `json:"hop"`
	Egress  string            `json:"egress"`
	BodyLen int64             `json:"body_len"`
}

type clusterRespFrame struct {
	ID         string               `json:"id"`
	StatusCode int                  `json:"status_code"`
	Header     map[string][]string  `json:"header"`
	BodyLen    int64                `json:"body_len"`
}

type clusterPeer struct {
	ID      string `json:"id"`
	Addr    string `json:"addr"`
	RTTms   int64  `json:"rtt_ms"`
	Dynamic bool   `json:"-"`
}

type clusterConfig struct {
	ID         string
	Token      string
	ListenAddr string
	JoinAddr   string
	Peers      []string
	FailoverOn map[int]bool
	FailTO     bool
	TunnelFile string
}

type pendingResp struct {
	rf     clusterRespFrame
	body   []byte
	err    error
	chunk  []byte
	isChunk bool
	done   bool
}

type pendingEntry struct {
	ch   chan pendingResp
	conn net.Conn
	streamCh chan pendingResp
	isStream bool
}

type clusterWireFrame struct {
	ID         string               `json:"id"`
	Method     string               `json:"method"`
	Path       string               `json:"path"`
	Headers    map[string]string    `json:"headers"`
	Visited    []string             `json:"visited"`
	Hop        int                  `json:"hop"`
	Egress     string               `json:"egress"`
	BodyLen    int64                `json:"body_len"`
	StatusCode *int                 `json:"status_code"`
	Header     map[string][]string  `json:"header"`
	IsChunk    bool                 `json:"is_chunk,omitempty"`
	Chunk      []byte               `json:"chunk,omitempty"`
	Done       bool                 `json:"done,omitempty"`
	Stream     bool                 `json:"stream,omitempty"`
}

type clusterNode struct {
	cfg      clusterConfig
	selfID   string
	mu       sync.RWMutex
	peers    map[string]*clusterPeer
	listener net.Listener
	joinConn net.Conn
	joinMu   sync.Mutex
	tlsCfg   *tls.Config
	peerConns    sync.Map
	inboundConns sync.Map
	connLocks    sync.Map
	pendingMu    sync.Mutex
	pending      map[string]pendingEntry
	onForward func(r *http.Request) (*http.Response, error)
	keepAlive time.Duration
	tunnels   *tunnelStore
}

func (n *clusterNode) SetForwarder(fn func(r *http.Request) (*http.Response, error)) { n.onForward = fn }
func (n *clusterNode) SelfID() string                                                { return n.selfID }
func (n *clusterNode) IsInternalPath(p string) bool                                  { return isClusterInternalPath(p) }
func (n *clusterNode) HandleHTTP(w http.ResponseWriter, r *http.Request) bool         { return handleClusterHTTP(w, r, n) }

func parseClusterArgs(args []string) (clusterConfig, []string) {
	cfg := clusterConfig{FailoverOn: map[int]bool{429: true, 502: true, 503: true, 504: true}, FailTO: true}
	var rest []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch a {
		case "--cluster-id":
			if i+1 < len(args) {
				i++
				cfg.ID = args[i]
			}
		case "--cluster-token":
			if i+1 < len(args) {
				i++
				cfg.Token = args[i]
			}
		case "--cluster-listen":
			if i+1 < len(args) {
				i++
				cfg.ListenAddr = args[i]
			}
		case "--cluster-join":
			if i+1 < len(args) {
				i++
				cfg.JoinAddr = args[i]
			}
		case "--peer":
			if i+1 < len(args) {
				i++
				cfg.Peers = append(cfg.Peers, args[i])
			}
		case "--failover-on":
			if i+1 < len(args) {
				i++
				cfg.FailoverOn = map[int]bool{}
				cfg.FailTO = false
				for _, s := range strings.Split(args[i], ",") {
					s = strings.TrimSpace(s)
					if strings.EqualFold(s, "timeout") {
						cfg.FailTO = true
						continue
					}
					var code int
					fmt.Sscanf(s, "%d", &code)
					if code != 0 {
						cfg.FailoverOn[code] = true
					}
				}
			}
		case "--tunnel-file":
			if i+1 < len(args) {
				i++
				cfg.TunnelFile = args[i]
			}
		default:
			rest = append(rest, a)
		}
	}
	return cfg, rest
}

func newClusterNode(cfg clusterConfig) *clusterNode {
	if cfg.ID == "" {
		cfg.ID = "node-" + randomHex(4)
	}
	cert, err := genSelfSignedCert()
	if err != nil {
		log.Printf("[cluster] 生成内存自签证书失败, 退化为明文: %v", err)
	}
	tlsCfg := &tls.Config{
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
	}
	return &clusterNode{
		cfg:     cfg,
		selfID:  cfg.ID,
		peers:   map[string]*clusterPeer{},
		tlsCfg:  tlsCfg,
		tunnels: newTunnelStore(cfg.TunnelFile),
		pending: map[string]pendingEntry{},
	}
}

func genSelfSignedCert() (tls.Certificate, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
	}, nil
}

func (n *clusterNode) Enabled() bool {
	return n.cfg.ListenAddr != "" || n.cfg.JoinAddr != "" || len(n.cfg.Peers) > 0
}

func (n *clusterNode) Start(forward func(r *http.Request) (*http.Response, error)) error {
	n.onForward = forward
	if n.keepAlive <= 0 {
		n.keepAlive = 60 * time.Second
	}
	if n.cfg.ListenAddr != "" {
		ln, err := net.Listen("tcp", n.cfg.ListenAddr)
		if err != nil {
			return err
		}
		n.listener = tls.NewListener(ln, n.tlsCfg)
		go n.acceptLoop()
	}
	for _, p := range n.cfg.Peers {
		n.mu.Lock()
		n.peers[p] = &clusterPeer{ID: p, Addr: p, RTTms: 9999}
		n.mu.Unlock()
	}
	if n.cfg.JoinAddr != "" {
		n.mu.Lock()
		if _, ok := n.peers[n.cfg.JoinAddr]; !ok {
			n.peers[n.cfg.JoinAddr] = &clusterPeer{ID: n.cfg.JoinAddr, Addr: n.cfg.JoinAddr, RTTms: 9999}
		}
		n.mu.Unlock()
	}
	if n.cfg.JoinAddr != "" {
		go n.joinLoop()
	}
	for _, p := range n.cfg.Peers {
		go n.loopPeerTunnel(p)
	}
	go n.probeLoop()
	return nil
}

func setTCPKeepAlive(c net.Conn, d time.Duration) {
	if tlsConn, ok := c.(*tls.Conn); ok {
		c = tlsConn.NetConn()
	}
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(d)
	}
}

func (n *clusterNode) loopPeerTunnel(addr string) {
	for {
		c, err := tls.Dial("tcp", addr, n.tlsCfg)
		if err != nil {
			log.Printf("[cluster] 拨号 peer %s 隧道失败: %v, 3s重试", addr, err)
			time.Sleep(3 * time.Second)
			continue
		}
		setTCPKeepAlive(c, n.keepAlive)
		n.peerConns.Store(addr, c)
		log.Printf("[cluster] peer %s 隧道已建立 (keepalive=%s)", addr, n.keepAlive)
		tid := "out-" + randomHex(4)
		n.tunnels.open(tid, "outbound", "", addr, c.LocalAddr().String())
		n.handleConn(c, tid)
		n.tunnels.close(tid)
		n.peerConns.Delete(addr)
		_ = c.Close()
		time.Sleep(3 * time.Second)
	}
}

func (n *clusterNode) acceptLoop() {
	for {
		c, err := n.listener.Accept()
		if err != nil {
			return
		}
		remote := c.RemoteAddr().String()
		local := c.LocalAddr().String()
		if tlsConn, ok := c.(*tls.Conn); ok {
			if hErr := tlsConn.Handshake(); hErr != nil {
				log.Printf("[cluster] 接受连接 TLS握手失败: %v", hErr)
				_ = c.Close()
				continue
			}
		}
		tid := "in-" + randomHex(4)
		n.tunnels.open(tid, "inbound", "", remote, local)
		log.Printf("[cluster] 接受隧道连接: id=%s remote=%s local=%s (服务端日志)", tid, remote, local)
		n.mu.Lock()
		n.peers[remote] = &clusterPeer{ID: remote, Addr: remote, RTTms: 9999, Dynamic: true}
		n.mu.Unlock()
		n.inboundConns.Store(remote, c)
		go func() {
			n.handleConn(c, tid)
			n.mu.Lock()
			delete(n.peers, remote)
			n.mu.Unlock()
			n.inboundConns.Delete(remote)
			log.Printf("[cluster] 隧道下线剔除: %s", remote)
		}()
	}
}

func (n *clusterNode) joinLoop() {
	for {
		tc, err := tls.Dial("tcp", n.cfg.JoinAddr, n.tlsCfg)
		if err != nil {
			log.Printf("[cluster] 加入地址 %s TLS握手失败: %v, 3s重试", n.cfg.JoinAddr, err)
			time.Sleep(3 * time.Second)
			continue
		}
		c := net.Conn(tc)
		log.Printf("[cluster] 节点 %s 与 %s TLS握手成功", n.selfID, n.cfg.JoinAddr)
		setTCPKeepAlive(c, n.keepAlive)
		log.Printf("[cluster] 节点 %s 已加入 %s (keepalive=%s)", n.selfID, n.cfg.JoinAddr, n.keepAlive)
		n.joinMu.Lock()
		n.joinConn = c
		n.joinMu.Unlock()
		tid := "join-" + randomHex(4)
		n.tunnels.open(tid, "outbound", "", n.cfg.JoinAddr, c.LocalAddr().String())
		n.handleConn(c, tid)
		n.tunnels.close(tid)
		log.Printf("[cluster] 节点 %s 与 %s 的连接已断开, 3s后重连", n.selfID, n.cfg.JoinAddr)
		n.joinMu.Lock()
		n.joinConn = nil
		n.joinMu.Unlock()
		time.Sleep(3 * time.Second)
	}
}

func (n *clusterNode) probeLoop() {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for range t.C {
		n.mu.RLock()
		list := make([]*clusterPeer, 0, len(n.peers))
		for _, p := range n.peers {
			list = append(list, p)
		}
		n.mu.RUnlock()
		for _, p := range list {
			start := time.Now()
			dummyURL, _ := url.Parse(clusterPingPath)
			dummyReq := &http.Request{Method: http.MethodGet, URL: dummyURL, Header: http.Header{}}
			visited := []string{n.selfID}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			resp, err := n.ForwardToPeer(ctx, p, dummyReq, nil, visited, 0)
			cancel()
			rtt := time.Since(start).Milliseconds()
			n.mu.Lock()
			peer := n.peers[p.ID]
			if peer != nil {
				peer.RTTms = rtt
				if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
					if resp != nil {
						resp.Body.Close()
					}
					if err != nil {
						log.Printf("[cluster] ping peer %s fail (tunnel): %v", p.Addr, err)
					} else {
						log.Printf("[cluster] ping peer %s status=%d (tunnel)", p.Addr, resp.StatusCode)
					}
					n.mu.Unlock()
					continue
				}
				resp.Body.Close()
				log.Printf("[cluster] ping peer %s ok rtt=%dms (tunnel)", p.Addr, rtt)
			}
			n.mu.Unlock()
		}
	}
}

func (n *clusterNode) handleConn(c net.Conn, tunnelID string) {
	defer c.Close()
	if tunnelID != "" {
		defer n.tunnels.close(tunnelID)
	}
	defer n.failPending(c)
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(n.keepAlive)
	}
	reader := bufio.NewReader(c)
	for {
		_ = c.SetReadDeadline(time.Now().Add(2 * n.keepAlive))
		var l int32
		if err := binary.Read(reader, binary.BigEndian, &l); err != nil {
			return
		}
		if l <= 0 || l > 1<<20 {
			return
		}
		buf := make([]byte, l)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return
		}
		var wf clusterWireFrame
		if err := json.Unmarshal(buf, &wf); err != nil {
			continue
		}
		if wf.IsChunk || wf.Done {
			if tunnelID != "" {
				n.tunnels.heartbeat(tunnelID)
			}
			n.deliverChunk(wf.ID, wf.Chunk, wf.Done, wf.StatusCode, wf.Header)
			continue
		}
		if wf.StatusCode != nil {
			var body []byte
			if wf.BodyLen > 0 {
				body = make([]byte, wf.BodyLen)
				if _, err := io.ReadFull(reader, body); err != nil {
					return
				}
			}
			if tunnelID != "" {
				n.tunnels.heartbeat(tunnelID)
			}
			n.deliverPending(wf.ID, wf.StatusCode, wf.Header, body)
			continue
		}
		if tunnelID != "" {
			n.tunnels.heartbeat(tunnelID)
		}
		if wf.Path == clusterPingPath {
			var discard []byte
			if wf.BodyLen > 0 {
				discard = make([]byte, wf.BodyLen)
				if _, err := io.ReadFull(reader, discard); err != nil {
					continue
				}
			}
			if wf.StatusCode == nil {
				respBody := []byte("pong")
				rf := clusterRespFrame{ID: wf.ID, StatusCode: 200, Header: map[string][]string{}, BodyLen: int64(len(respBody))}
				rfb, _ := json.Marshal(rf)
				var rlb [4]byte
				binary.BigEndian.PutUint32(rlb[:], uint32(len(rfb)))
				mu := n.connLock(c)
				mu.Lock()
				_, _ = c.Write(rlb[:])
				_, _ = c.Write(rfb)
				_, _ = c.Write(respBody)
				mu.Unlock()
			}
			continue
		}
		h := clusterFrameHeader{
			ID:      wf.ID,
			Method:  wf.Method,
			Path:    wf.Path,
			Headers: wf.Headers,
			Visited: wf.Visited,
			Hop:     wf.Hop,
			Egress:  wf.Egress,
			BodyLen: wf.BodyLen,
		}
		if h.Hop > maxHop {
			continue
		}
		visited := append([]string(nil), h.Visited...)
		visited = append(visited, n.selfID)
		h.Visited = visited
		h.Hop = h.Hop + 1
		u, _ := url.Parse(h.Path)
		if u == nil || u.Path == "" {
			u = &url.URL{Path: "/"}
		}
		fwdReq := &http.Request{Method: h.Method, URL: u, Header: http.Header{}}
		for k, v := range h.Headers {
			fwdReq.Header.Set(k, v)
		}
		fwdReq.Header.Set(clusterHdrVisited, strings.Join(h.Visited, ","))
		fwdReq.Header.Set(clusterHdrHop, fmt.Sprintf("%d", h.Hop))
		fwdReq.Header.Set(clusterHdrToken, n.cfg.Token)
		fwdReq.Header.Set(clusterHdrEgress, h.Egress)
		var reqBody []byte
		if h.BodyLen > 0 {
			reqBody = make([]byte, h.BodyLen)
			if _, err := io.ReadFull(reader, reqBody); err != nil {
				continue
			}
		}
		fwdReq.Body = io.NopCloser(bytes.NewReader(reqBody))
		fwdReq.ContentLength = h.BodyLen
		var resp *http.Response
		var ferr error
		if n.onForward == nil {
			log.Printf("[cluster] 服务端未配置转发器, 丢弃帧 id=%s method=%s path=%s", h.ID, h.Method, h.Path)
			ferr = fmt.Errorf("服务端未配置转发器, 无法处理集群帧")
		} else {
			log.Printf("[cluster] 转发请求 id=%s method=%s path=%s headers=%v stream=%v", h.ID, fwdReq.Method, fwdReq.URL.RequestURI(), fwdReq.Header, wf.Stream)
			resp, ferr = n.onForward(fwdReq)
		}
		if wf.Stream && resp != nil && ferr == nil && resp.StatusCode == 200 && isSSEHeader(resp.Header) {
			hdrs := cloneHeaderMap(resp.Header)
			mu := n.connLock(c)
			mu.Lock()
			initFrame := clusterWireFrame{ID: h.ID, StatusCode: intPtr(200), Header: hdrs, IsChunk: false, Stream: true}
			if err := writeFrame(c, initFrame, nil); err != nil {
				mu.Unlock()
				resp.Body.Close()
				return
			}
			mu.Unlock()
			buf := make([]byte, 4096)
			for {
				nbytes, rerr := resp.Body.Read(buf)
				if nbytes > 0 {
					chunk := make([]byte, nbytes)
					copy(chunk, buf[:nbytes])
					mu.Lock()
					cf := clusterWireFrame{ID: h.ID, IsChunk: true, Chunk: chunk}
					werr := writeFrame(c, cf, nil)
					mu.Unlock()
					if werr != nil {
						break
					}
				}
				if rerr != nil {
					break
				}
			}
			resp.Body.Close()
			mu.Lock()
			doneFrame := clusterWireFrame{ID: h.ID, Done: true}
			_ = writeFrame(c, doneFrame, nil)
			mu.Unlock()
			continue
		}
		var respBody []byte
		if resp != nil && resp.Body != nil {
			respBody, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		var hdrs map[string][]string
		if resp != nil {
			hdrs = make(map[string][]string)
			for k, vv := range resp.Header {
				if len(vv) == 0 {
					hdrs[k] = []string{}
					continue
				}
				cp := make([]string, len(vv))
				copy(cp, vv)
				hdrs[k] = cp
			}
		}
		rf := clusterRespFrame{
			ID:         h.ID,
			StatusCode: 502,
			Header:     hdrs,
			BodyLen:    int64(len(respBody)),
		}
		if ferr != nil {
			rf.BodyLen = int64(len([]byte(ferr.Error())))
		} else if resp != nil {
			rf.StatusCode = resp.StatusCode
		}
		rfb, _ := json.Marshal(rf)
		var rlb [4]byte
		binary.BigEndian.PutUint32(rlb[:], uint32(len(rfb)))
		mu := n.connLock(c)
		mu.Lock()
		if _, err := c.Write(rlb[:]); err != nil {
			mu.Unlock()
			return
		}
		if _, err := c.Write(rfb); err != nil {
			mu.Unlock()
			return
		}
		if ferr != nil {
			if _, err := c.Write([]byte(ferr.Error())); err != nil {
				mu.Unlock()
				return
			}
		} else if respBody != nil {
			if _, err := c.Write(respBody); err != nil {
				mu.Unlock()
				return
			}
		}
		mu.Unlock()
	}
}

func (n *clusterNode) ShouldFailover(status int, isTimeout bool) bool {
	if isTimeout && n.cfg.FailTO {
		return true
	}
	_, ok := n.cfg.FailoverOn[status]
	return ok
}

func (n *clusterNode) PickPeers(visited map[string]bool) []*clusterPeer {
	n.mu.RLock()
	defer n.mu.RUnlock()
	var list []*clusterPeer
	for _, p := range n.peers {
		if visited[p.ID] {
			continue
		}
		list = append(list, p)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].RTTms < list[j].RTTms })
	return list
}

func (n *clusterNode) ForwardToPeer(ctx context.Context, peer *clusterPeer, orig *http.Request, body []byte, visited []string, hop int) (*http.Response, error) {
	n.joinMu.Lock()
	jc := n.joinConn
	n.joinMu.Unlock()
	if jc != nil && n.cfg.JoinAddr != "" && peer.Addr == n.cfg.JoinAddr {
		return n.forwardViaFrame(ctx, jc, orig, body, visited, hop)
	}
	if v, ok := n.peerConns.Load(peer.Addr); ok {
		if c := v.(net.Conn); c != nil {
			return n.forwardViaFrame(ctx, c, orig, body, visited, hop)
		}
	}
	if v, ok := n.inboundConns.Load(peer.Addr); ok {
		if c := v.(net.Conn); c != nil {
			return n.forwardViaFrame(ctx, c, orig, body, visited, hop)
		}
	}
	return nil, fmt.Errorf("no tls tunnel to peer %s", peer.Addr)
}

func (n *clusterNode) connLock(c net.Conn) *sync.Mutex {
	if c == nil {
		return &sync.Mutex{}
	}
	key := fmt.Sprintf("%p", c)
	v, _ := n.connLocks.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func (n *clusterNode) forwardViaFrame(ctx context.Context, conn net.Conn, orig *http.Request, body []byte, visited []string, hop int) (*http.Response, error) {
	mu := n.connLock(conn)
	reqID := "r-" + randomHex(8)
	isStream := bytes.Contains(bytes.ToLower(body), []byte(`"stream":true`)) || bytes.Contains(body, []byte(`"stream": true`))
	ch := make(chan pendingResp, 1)
	var streamCh chan pendingResp
	if isStream {
		streamCh = make(chan pendingResp, 64)
	}
	n.pendingMu.Lock()
	n.pending[reqID] = pendingEntry{ch: ch, conn: conn, streamCh: streamCh, isStream: isStream}
	n.pendingMu.Unlock()
	defer func() {
		n.pendingMu.Lock()
		delete(n.pending, reqID)
		n.pendingMu.Unlock()
	}()

	wf := clusterWireFrame{
		ID:      reqID,
		Method:  orig.Method,
		Path:    orig.URL.RequestURI(),
		Visited: visited,
		Hop:     hop,
		Egress:  orig.Header.Get(clusterHdrEgress),
		BodyLen: int64(len(body)),
		Stream:  isStream,
		Headers: map[string]string{},
	}
	for k, vv := range orig.Header {
		switch strings.ToLower(k) {
		case strings.ToLower(clusterHdrVisited), strings.ToLower(clusterHdrHop), strings.ToLower(clusterHdrToken), strings.ToLower(clusterHdrEgress):
			continue
		}
		if len(vv) > 0 {
			wf.Headers[k] = vv[0]
		}
	}
	frame, _ := json.Marshal(wf)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(frame)))
	mu.Lock()
	if _, err := conn.Write(lb[:]); err != nil {
		mu.Unlock()
		return nil, err
	}
	if _, err := conn.Write(frame); err != nil {
		mu.Unlock()
		return nil, err
	}
	if len(body) > 0 {
		if _, err := conn.Write(body); err != nil {
			mu.Unlock()
			return nil, err
		}
	}
	mu.Unlock()

	select {
	case pr := <-ch:
		if pr.err != nil {
			return nil, pr.err
		}
		if isStream && pr.rf.Header != nil {
			if ct, ok := pr.rf.Header["Content-Type"]; ok {
				for _, v := range ct {
					if strings.Contains(v, "event-stream") {
						pr2 := <-streamCh
						_ = pr2
						hdr := http.Header{}
						for k, vv := range pr.rf.Header {
							for _, v := range vv {
								hdr.Add(k, v)
							}
						}
						pr2 = <-ch
						if pr2.err != nil {
							return nil, pr2.err
						}
						return buildStreamResponse(pr.rf, streamCh), nil
					}
				}
			}
		}
		if isStream && streamCh != nil {
			select {
			case first := <-streamCh:
				if first.isChunk {
					hdr := http.Header{}
					for k, vv := range pr.rf.Header {
						for _, v := range vv {
							hdr.Add(k, v)
						}
					}
					return buildStreamResponse(pr.rf, streamChWithFirst(streamCh, first)), nil
				}
				if first.done {
					hdr := http.Header{}
					for k, vv := range pr.rf.Header {
						for _, v := range vv {
							hdr.Add(k, v)
						}
					}
					var respBody []byte
					if pr.rf.BodyLen > 0 {
						respBody = pr.body
					}
					return &http.Response{StatusCode: pr.rf.StatusCode, Header: hdr, Body: io.NopCloser(bytes.NewReader(respBody))}, nil
				}
			default:
			}
		}
		var respBody []byte
		if pr.rf.BodyLen > 0 {
			respBody = pr.body
		}
		hdr := http.Header{}
		for k, vv := range pr.rf.Header {
			for _, v := range vv {
				hdr.Add(k, v)
			}
		}
		return &http.Response{StatusCode: pr.rf.StatusCode, Header: hdr, Body: io.NopCloser(bytes.NewReader(respBody))}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *clusterNode) deliverPending(id string, status *int, hdr map[string][]string, body []byte) {
	n.pendingMu.Lock()
	pe := n.pending[id]
	n.pendingMu.Unlock()
	if pe.ch == nil {
		return
	}
	select {
	case pe.ch <- pendingResp{rf: clusterRespFrame{ID: id, StatusCode: *status, Header: hdr, BodyLen: int64(len(body))}, body: body}:
	default:
	}
}

func (n *clusterNode) deliverChunk(id string, chunk []byte, done bool, status *int, hdr map[string][]string) {
	n.pendingMu.Lock()
	pe := n.pending[id]
	n.pendingMu.Unlock()
	if pe.streamCh == nil {
		if done {
			select {
			case pe.ch <- pendingResp{done: true}:
			default:
			}
		} else if chunk != nil {
			select {
			case pe.streamCh <- pendingResp{chunk: chunk, isChunk: true}:
			default:
			}
		}
		return
	}
	if status != nil {
		select {
		case pe.ch <- pendingResp{rf: clusterRespFrame{ID: id, StatusCode: *status, Header: hdr}}:
		default:
		}
		return
	}
	if done {
		select {
		case pe.streamCh <- pendingResp{done: true}:
		default:
		}
		select {
		case pe.ch <- pendingResp{done: true}:
		default:
		}
		return
	}
	select {
	case pe.streamCh <- pendingResp{chunk: chunk, isChunk: true}:
	default:
	}
}

func (n *clusterNode) failPending(c net.Conn) {
	n.pendingMu.Lock()
	for id, pe := range n.pending {
		if pe.conn != c {
			continue
		}
		select {
		case pe.ch <- pendingResp{err: fmt.Errorf("集群隧道连接已关闭")}:
		default:
		}
		if pe.streamCh != nil {
			select {
			case pe.streamCh <- pendingResp{err: fmt.Errorf("集群隧道连接已关闭")}:
			default:
			}
		}
		delete(n.pending, id)
	}
	n.pendingMu.Unlock()
}

func buildStreamResponse(rf clusterRespFrame, ch chan pendingResp) *http.Response {
	hdr := http.Header{}
	for k, vv := range rf.Header {
		for _, v := range vv {
			hdr.Add(k, v)
		}
	}
	pr, pw := io.Pipe()
	go func() {
		for item := range ch {
			if item.err != nil {
				_ = pw.CloseWithError(item.err)
				return
			}
			if item.done {
				_ = pw.Close()
				return
			}
			if item.isChunk && len(item.chunk) > 0 {
				if _, err := pw.Write(item.chunk); err != nil {
					return
				}
			}
		}
		_ = pw.Close()
	}()
	return &http.Response{StatusCode: rf.StatusCode, Header: hdr, Body: pr}
}

func streamChWithFirst(ch chan pendingResp, first pendingResp) chan pendingResp {
	out := make(chan pendingResp, 64)
	out <- first
	go func() {
		for v := range ch {
			out <- v
		}
		close(out)
	}()
	return out
}

func writeFrame(c net.Conn, wf clusterWireFrame, body []byte) error {
	b, _ := json.Marshal(wf)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
	if _, err := c.Write(lb[:]); err != nil {
		return err
	}
	if _, err := c.Write(b); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := c.Write(body); err != nil {
			return err
		}
	}
	return nil
}

func cloneHeaderMap(h http.Header) map[string][]string {
	m := make(map[string][]string, len(h))
	for k, vv := range h {
		cp := make([]string, len(vv))
		copy(cp, vv)
		m[k] = cp
	}
	return m
}

func isSSEHeader(h http.Header) bool {
	ct := h.Get("Content-Type")
	return strings.Contains(ct, "text/event-stream") || strings.Contains(ct, "event-stream")
}

func intPtr(v int) *int { return &v }

func isClusterInternalPath(p string) bool {
	switch p {
	case clusterForwardPath, clusterPingPath, "/_cluster/peers":
		return true
	}
	return false
}

func parseVisited(r *http.Request) ([]string, int) {
	visitedStr := r.Header.Get(clusterHdrVisited)
	parts := []string{}
	if visitedStr != "" {
		parts = strings.Split(visitedStr, ",")
	}
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.TrimSpace(parts[i])
	}
	hop := 0
	fmt.Sscanf(r.Header.Get(clusterHdrHop), "%d", &hop)
	return parts, hop
}

func buildVisited(self string, in []string, hop int) ([]string, int) {
	visited := append([]string(nil), in...)
	visited = append(visited, self)
	return visited, hop + 1
}

func handleClusterHTTP(w http.ResponseWriter, r *http.Request, node *clusterNode) bool {
	if node == nil || !node.Enabled() {
		return false
	}
	switch r.URL.Path {
	case "/_cluster/peers":
		node.mu.RLock()
		list := make([]*clusterPeer, 0, len(node.peers))
		for _, p := range node.peers {
			list = append(list, p)
		}
		node.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(list)
		return true
	case clusterForwardPath:
		if node.cfg.Token != "" && !secureCompare(r.Header.Get(clusterHdrToken), node.cfg.Token) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return true
		}
		if node == nil || node.onForward == nil {
			http.Error(w, "cluster not enabled", http.StatusNotImplemented)
			return true
		}
		visitedStr := r.Header.Get(clusterHdrVisited)
		var visited []string
		if visitedStr != "" {
			visited = strings.Split(visitedStr, ",")
		}
		for _, v := range visited {
			if strings.TrimSpace(v) == node.selfID {
				http.Error(w, "loop detected", http.StatusLoopDetected)
				return true
			}
		}
		hop := 0
		fmt.Sscanf(r.Header.Get(clusterHdrHop), "%d", &hop)
		if hop > maxHop {
			http.Error(w, "max hop exceeded", http.StatusLoopDetected)
			return true
		}
		var body []byte
		if r.Body != nil {
			body, _ = io.ReadAll(r.Body)
		}
		fwdMethod := r.Header.Get("X-Forwarded-Method")
		fwdURI := r.Header.Get("X-Forwarded-Uri")
		if fwdMethod == "" {
			fwdMethod = http.MethodGet
		}
		u, _ := url.Parse(fwdURI)
		if u == nil || u.Path == "" {
			u = &url.URL{Path: "/"}
		}
		fwdReq := &http.Request{Method: fwdMethod, URL: u, Header: http.Header{}}
		for k, vv := range r.Header {
			lk := strings.ToLower(k)
			if strings.HasPrefix(lk, "x-forwarded-") || lk == strings.ToLower(clusterHdrVisited) || lk == strings.ToLower(clusterHdrHop) || lk == strings.ToLower(clusterHdrToken) || lk == strings.ToLower(clusterHdrEgress) {
				continue
			}
			for _, v := range vv {
				fwdReq.Header.Add(k, v)
			}
		}
		fwdReq.Header.Set(clusterHdrVisited, strings.Join(visited, ","))
		fwdReq.Header.Set(clusterHdrHop, fmt.Sprintf("%d", hop))
		fwdReq.Header.Set(clusterHdrToken, node.cfg.Token)
		fwdReq.Header.Set(clusterHdrEgress, r.Header.Get(clusterHdrEgress))
		fwdReq.Header.Set("X-Forwarded-Method", "")
		fwdReq.Header.Set("X-Forwarded-Uri", "")
		if len(body) > 0 {
			fwdReq.Body = io.NopCloser(bytes.NewReader(body))
			fwdReq.ContentLength = int64(len(body))
		}
		if node.onForward == nil {
			http.Error(w, "cluster not enabled", http.StatusNotImplemented)
			return true
		}
		resp, err := node.onForward(fwdReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return true
		}
		defer resp.Body.Close()
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return true
	}
	return false
}
