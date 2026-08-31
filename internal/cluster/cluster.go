package cluster

import (
	"bufio"
	"opencode-zen-proxy/internal/tunnel"
	"opencode-zen-proxy/internal/util"
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

	"github.com/gorilla/websocket"
)

const (
	PingPath    = "/_cluster/ping"
	MaxHop             = 3
	HdrVisited  = "X-Cluster-Visited"
	HdrHop      = "X-Cluster-Hop"
	HdrToken    = "X-Cluster-Token"
	HdrEgress   = "X-Cluster-Egress"
)

type frameHeader struct {
	ID      string            `json:"id"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Visited []string          `json:"visited"`
	Hop     int               `json:"hop"`
	Egress  string            `json:"egress"`
	BodyLen int64             `json:"body_len"`
}

type respFrame struct {
	ID         string               `json:"id"`
	StatusCode int                  `json:"status_code"`
	Header     map[string][]string  `json:"header"`
	BodyLen    int64                `json:"body_len"`
}

type Peer struct {
	ID               string    `json:"id"`
	Addr             string    `json:"addr"`
	WSSAddr          string    `json:"wss_addr,omitempty"`
	RTTms            int64     `json:"rtt_ms"`
	NodeID           string    `json:"node_id,omitempty"`
	Dynamic          bool      `json:"-"`
	FailCount        int       `json:"-"`
	UnavailableUntil time.Time `json:"-"`
}

type Config struct {
	ID          string
	Token       string
	ListenAddr  string
	JoinAddr    string
	JoinWSSAddr string
	FailoverOn  map[int]bool
	FailTO      bool
	TunnelFile  string
}

type pendingResp struct {
	rf     respFrame
	body   []byte
	err    error
	chunk  []byte
	isChunk bool
	done   bool
}

type pendingEntry struct {
	ch        chan pendingResp
	conn      net.Conn
	streamCh  chan pendingResp
	isStream  bool
	overflow  int
}

type WireFrame struct {
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
	AuthToken  string               `json:"auth_token,omitempty"`
	NodeID     string               `json:"node_id,omitempty"`
}

type connWriter struct {
	conn net.Conn
	ch   chan []byte
	done chan struct{}
	mu   sync.Mutex
	once sync.Once
}

type Node struct {
	cfg      Config
	selfID   string
	mu       sync.RWMutex
	peers    map[string]*Peer
	listener net.Listener
	joinConn net.Conn
	joinMu   sync.Mutex
	tlsCfg   *tls.Config
	peerConns    sync.Map
	inboundConns sync.Map
	connWriters  sync.Map
	pendingMu    sync.Mutex
	pending      map[string]*pendingEntry
	onForward func(r *http.Request) (*http.Response, error)
	keepAlive time.Duration
	Tunnels *tunnel.Store
}

func (n *Node) SetForwarder(fn func(r *http.Request) (*http.Response, error)) { n.onForward = fn }
func (n *Node) SelfID() string                                                { return n.selfID }
func (n *Node) IsInternalPath(p string) bool                                  { return isClusterInternalPath(p) }
func (n *Node) HandleHTTP(w http.ResponseWriter, r *http.Request) bool         { return handleClusterHTTP(w, r, n) }

func ParseClusterArgs(args []string) (Config, []string) {
	cfg := Config{FailoverOn: map[int]bool{403: true, 429: true, 502: true, 503: true, 504: true}, FailTO: true}
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
				parts := strings.Split(args[i], ",")
				cfg.JoinAddr = parts[0]
				if len(parts) > 1 {
					cfg.JoinWSSAddr = parts[1]
				}
			}
		case "--peer":
			if i+1 < len(args) {
				i++
			}
			log.Printf("[cluster] --peer 已移除, 请用 --cluster-join/--cluster-listen (隧道模式)")
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

func NewNode(cfg Config) *Node {
	if cfg.ID == "" {
		cfg.ID = "node-" + util.RandomHex(4)
	}
	cert, err := genSelfSignedCert()
	if err != nil {
		log.Printf("[cluster] 生成内存自签证书失败, 退化为明文: %v", err)
	}
	tlsCfg := &tls.Config{
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
	}
	return &Node{
		cfg:     cfg,
		selfID:  cfg.ID,
		peers:   map[string]*Peer{},
		tlsCfg:  tlsCfg,
		Tunnels: tunnel.NewStore(cfg.TunnelFile),
		pending: map[string]*pendingEntry{},
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

func (n *Node) Enabled() bool {
	return n.cfg.ListenAddr != "" || n.cfg.JoinAddr != ""
}

func (n *Node) Start(forward func(r *http.Request) (*http.Response, error)) error {
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
	if n.cfg.JoinAddr != "" {
		n.mu.Lock()
		if _, ok := n.peers[n.cfg.JoinAddr]; !ok {
			n.peers[n.cfg.JoinAddr] = &Peer{ID: n.cfg.JoinAddr, Addr: n.cfg.JoinAddr, RTTms: 9999}
		}
		n.mu.Unlock()
	}
	if n.cfg.JoinAddr != "" {
		go n.joinLoop()
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

func (n *Node) getWriter(c net.Conn) *connWriter {
	key := fmt.Sprintf("%p", c)
	if v, ok := n.connWriters.Load(key); ok {
		return v.(*connWriter)
	}
	cw := &connWriter{conn: c, ch: make(chan []byte, 256), done: make(chan struct{})}
	actual, loaded := n.connWriters.LoadOrStore(key, cw)
	if loaded {
		return actual.(*connWriter)
	}
	go func() {
		for b := range cw.ch {
			cw.mu.Lock()
			if _, err := c.Write(b); err != nil {
				cw.mu.Unlock()
				return
			}
			cw.mu.Unlock()
		}
		close(cw.done)
	}()
	return cw
}

func (n *Node) removeWriter(c net.Conn) {
	key := fmt.Sprintf("%p", c)
	if v, ok := n.connWriters.Load(key); ok {
		cw := v.(*connWriter)
		n.connWriters.Delete(key)
		cw.once.Do(func() { close(cw.ch) })
	}
}

func (n *Node) writeConn(c net.Conn, data []byte) error {
	cw := n.getWriter(c)
	cp := make([]byte, len(data))
	copy(cp, data)
	select {
	case cw.ch <- cp:
		return nil
	default:
		select {
		case cw.ch <- cp:
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("conn write queue full")
		}
	}
}

func (n *Node) dispatchFrame(c net.Conn, isWS bool, head, body []byte, tunnelID string) {
	var wf WireFrame
	if err := json.Unmarshal(head, &wf); err != nil {
		return
	}
	if wf.IsChunk || wf.Done {
		if tunnelID != "" {
			n.Tunnels.Heartbeat(tunnelID)
		}
		n.deliverChunk(wf.ID, wf.Chunk, wf.Done, wf.StatusCode, wf.Header)
		return
	}
	if wf.StatusCode != nil {
		if strings.HasPrefix(wf.ID, "ping-") {
			if tunnelID != "" {
				n.Tunnels.Heartbeat(tunnelID)
			}
			return
		}
		if wf.BodyLen > 0 && !isWS {
			b := make([]byte, wf.BodyLen)
			if _, err := io.ReadFull(c, b); err != nil {
				return
			}
			body = b
		}
		if tunnelID != "" {
			n.Tunnels.Heartbeat(tunnelID)
		}
		n.deliverPending(wf.ID, wf.StatusCode, wf.Header, body)
		return
	}
	if tunnelID != "" {
		n.Tunnels.Heartbeat(tunnelID)
	}
	if wf.Path == PingPath {
		if wf.StatusCode == nil {
			respBody := []byte("pong")
			rf := respFrame{ID: wf.ID, StatusCode: 200, Header: map[string][]string{}, BodyLen: int64(len(respBody))}
			rfb, _ := json.Marshal(rf)
			var rlb [4]byte
			binary.BigEndian.PutUint32(rlb[:], uint32(len(rfb)))
			packed := make([]byte, 0, 4+len(rfb)+len(respBody))
			packed = append(packed, rlb[:]...)
			packed = append(packed, rfb...)
			packed = append(packed, respBody...)
			_ = n.writeConn(c, packed)
			log.Printf("[cluster] send pong id=%s to %s tunnel=%s", wf.ID, c.RemoteAddr(), tunnelID)
		}
		return
	}
	h := frameHeader{
		ID:      wf.ID,
		Method:  wf.Method,
		Path:    wf.Path,
		Headers: wf.Headers,
		Visited: wf.Visited,
		Hop:     wf.Hop,
		Egress:  wf.Egress,
		BodyLen: wf.BodyLen,
	}
	visited := append([]string(nil), h.Visited...)
	visited = append(visited, n.selfID)
	h.Visited = visited
	u, _ := url.Parse(h.Path)
	if u == nil || u.Path == "" {
		u = &url.URL{Path: "/"}
	}
	fwdReq := &http.Request{Method: h.Method, URL: u, Header: http.Header{}}
	for k, v := range h.Headers {
		fwdReq.Header.Set(k, v)
	}
	fwdReq.Header.Set(HdrVisited, strings.Join(h.Visited, ","))
	fwdReq.Header.Set(HdrHop, fmt.Sprintf("%d", h.Hop))
	fwdReq.Header.Set(HdrToken, n.cfg.Token)
	fwdReq.Header.Set(HdrEgress, h.Egress)
	var reqBody []byte
	if h.BodyLen > 0 {
		reqBody = body
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
		initFrame := WireFrame{ID: h.ID, StatusCode: intPtr(200), Header: hdrs, IsChunk: false, Stream: true}
		if err := n.writeConn(c, packFrame(initFrame, nil)); err != nil {
			resp.Body.Close()
			return
		}
		buf := make([]byte, 4096)
		for {
			nbytes, rerr := resp.Body.Read(buf)
			if nbytes > 0 {
				chunk := make([]byte, nbytes)
				copy(chunk, buf[:nbytes])
				cf := WireFrame{ID: h.ID, IsChunk: true, Chunk: chunk}
				if werr := n.writeConn(c, packFrame(cf, nil)); werr != nil {
					break
				}
			}
			if rerr != nil {
				break
			}
		}
		resp.Body.Close()
		doneFrame := WireFrame{ID: h.ID, Done: true}
		_ = n.writeConn(c, packFrame(doneFrame, nil))
		return
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
	rf := respFrame{
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
	var bodyOut []byte
	if ferr != nil {
		bodyOut = []byte(ferr.Error())
	} else {
		bodyOut = respBody
	}
	packed := make([]byte, 0, 4+len(rfb)+len(bodyOut))
	packed = append(packed, rlb[:]...)
	packed = append(packed, rfb...)
	packed = append(packed, bodyOut...)
	if err := n.writeConn(c, packed); err != nil {
		return
	}
}

func packFrame(wf WireFrame, body []byte) []byte {
	b, _ := json.Marshal(wf)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
	out := make([]byte, 0, 4+len(b)+len(body))
	out = append(out, lb[:]...)
	out = append(out, b...)
	out = append(out, body...)
	return out
}

func cleanAddr(addr string) string {
	return strings.TrimPrefix(strings.TrimPrefix(addr, "wss://"), "ws://")
}

func (n *Node) dialWSS(addr string) (net.Conn, string, error) {
	dialer := websocket.Dialer{
		TLSClientConfig:  n.tlsCfg,
		HandshakeTimeout: 10 * time.Second,
		NetDialContext:   (&net.Dialer{Timeout: 10 * time.Second}).DialContext,
	}
	ws, _, err := dialer.Dial(addr, nil)
	if err != nil {
		return nil, "", err
	}
	c := &wsConnWrapper{conn: ws}
	if peerNodeID, err := n.handshake(c, true); err != nil {
		_ = c.Close()
		return nil, "", err
	} else {
		return c, peerNodeID, nil
	}
}

	var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (n *Node) acceptLoop() {
	for {
		c, err := n.listener.Accept()
		if err != nil {
			return
		}
		remote := c.RemoteAddr().String()
		log.Printf("[cluster] 收到入站连接: from=%s local=%s", remote, c.LocalAddr())

		// 简单的 HTTP/WS 握手嗅探 (Peek 限时, 防连接后不发数据的连接阻塞 acceptLoop)
		br := bufio.NewReader(c)
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		peek, perr := br.Peek(4)
		_ = c.SetReadDeadline(time.Time{})
		if perr != nil {
			_ = c.Close()
			continue
		}
		if len(peek) >= 3 && string(peek[:3]) == "GET" {
			peersMu := &n.mu
			peersMap := n.peers
			inboundMap := &n.inboundConns
			go func(c net.Conn, br *bufio.Reader) {
				_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
				req, _ := http.ReadRequest(br)
				_ = c.SetReadDeadline(time.Time{})
				if req == nil {
					c.Close()
					return
				}
				ws, err := wsUpgrader.Upgrade(&fakeResponseWriter{conn: c, br: br}, req, nil)
				if err != nil {
					log.Printf("[cluster] WS 升级失败: from=%s err=%v", remote, err)
					c.Close()
					return
				}
				conn := &wsConnWrapper{conn: ws}
				peerNodeID, vErr := n.verifyHandshake(conn, true)
				if vErr != nil {
					log.Printf("[cluster] WSS 握手被拒: from=%s err=%v", remote, vErr)
					_ = conn.Close()
					return
				}
				if peerNodeID != "" {
					peersMu.Lock()
					wsID := c.RemoteAddr().String()
					peersMap[wsID] = &Peer{ID: wsID, Addr: wsID, RTTms: 9999, NodeID: peerNodeID, Dynamic: true}
					peersMu.Unlock()
					inboundMap.Store(wsID, conn)
					tid := "ws-" + util.RandomHex(4)
					n.Tunnels.Open(tid, "inbound", peerNodeID, wsID, conn.LocalAddr().String())
					n.handleConn(conn, true, tid)
					peersMu.Lock()
					delete(peersMap, wsID)
					peersMu.Unlock()
					inboundMap.Delete(wsID)
					return
				}
				inboundMap.Store(c.RemoteAddr().String(), conn)
				tid2 := "ws-" + util.RandomHex(4)
				n.Tunnels.Open(tid2, "inbound", "", c.RemoteAddr().String(), conn.LocalAddr().String())
				n.handleConn(conn, true, tid2)
				inboundMap.Delete(c.RemoteAddr().String())
			}(c, br)
			continue
		}

		// 原生 TCP 隧道
		wrapped := &tcpConnWrapper{Conn: c, Reader: io.MultiReader(br, c)}
		go n.handleRawConn(wrapped, func(conn net.Conn, tid string) {
			n.handleConn(conn, false, tid)
			n.mu.Lock()
			delete(n.peers, conn.RemoteAddr().String())
			n.mu.Unlock()
			n.inboundConns.Delete(conn.RemoteAddr().String())
			util.LogDebugf("[cluster] 隧道下线剔除: %s", conn.RemoteAddr())
		})
	}
}

func (n *Node) handleRawConn(c net.Conn, onDone func(net.Conn, string)) {
	remote := c.RemoteAddr().String()
	local := c.LocalAddr().String()
	underlying := c
	if tcw, ok := c.(*tcpConnWrapper); ok {
		underlying = tcw.Conn
	}
	if tlsConn, ok := underlying.(*tls.Conn); ok {
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		if hErr := tlsConn.Handshake(); hErr != nil {
			log.Printf("[cluster] 接受连接 TLS握手失败: %v", hErr)
			_ = c.Close()
			return
		}
		_ = c.SetDeadline(time.Time{})
	}
	peerNodeID, err := n.verifyHandshake(c, false)
	if err != nil {
		log.Printf("[cluster] 匿名裸TCP 连接被拒绝: %v (from %s)", err, remote)
		_ = c.Close()
		return
	}
	tid := "in-" + util.RandomHex(4)
	n.Tunnels.Open(tid, "inbound", "", remote, local)
	log.Printf("[cluster] 接受隧道连接: id=%s remote=%s local=%s peerID=%s (服务端日志)", tid, remote, local, peerNodeID)
	n.mu.Lock()
	n.peers[remote] = &Peer{ID: remote, Addr: remote, RTTms: 9999, NodeID: peerNodeID, Dynamic: true}
	n.mu.Unlock()
	n.inboundConns.Store(remote, c)
	onDone(c, tid)
}

type wsConnWrapper struct {
	conn *websocket.Conn
}

func (w *wsConnWrapper) Read(b []byte) (int, error) {
	_, r, err := w.conn.NextReader()
	if err != nil {
		return 0, err
	}
	return r.Read(b)
}

func (w *wsConnWrapper) Write(b []byte) (int, error) {
	err := w.conn.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (w *wsConnWrapper) Close() error               { return w.conn.Close() }
func (w *wsConnWrapper) LocalAddr() net.Addr       { return w.conn.LocalAddr() }
func (w *wsConnWrapper) RemoteAddr() net.Addr      { return w.conn.RemoteAddr() }
func (w *wsConnWrapper) SetDeadline(t time.Time) error      { return w.conn.UnderlyingConn().SetDeadline(t) }
func (w *wsConnWrapper) SetReadDeadline(t time.Time) error  { return w.conn.UnderlyingConn().SetReadDeadline(t) }
func (w *wsConnWrapper) SetWriteDeadline(t time.Time) error { return w.conn.UnderlyingConn().SetWriteDeadline(t) }

type tcpConnWrapper struct {
	net.Conn
	io.Reader
}

func (t *tcpConnWrapper) Read(b []byte) (int, error) { return t.Reader.Read(b) }

func (n *Node) handshake(c net.Conn, isWS bool) (string, error) {
	frame := WireFrame{AuthToken: n.cfg.Token, NodeID: n.selfID}
	b, _ := json.Marshal(frame)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
	framed := append(lb[:], b...)
	var err error
	if isWS {
		err = c.(*wsConnWrapper).conn.WriteMessage(websocket.BinaryMessage, framed)
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if err == nil {
			_, data, rerr := c.(*wsConnWrapper).conn.ReadMessage()
			_ = c.SetReadDeadline(time.Time{})
			if rerr != nil {
				return "", rerr
			}
			if len(data) < 4 {
				return "", fmt.Errorf("握手响应长度非法")
			}
			hl := binary.BigEndian.Uint32(data[:4])
			if hl == 0 || hl > 1<<10 || int(hl)+4 > len(data) {
				return "", fmt.Errorf("握手响应长度非法")
			}
			var resp WireFrame
			if err2 := json.Unmarshal(data[4:4+hl], &resp); err2 != nil {
				return "", err2
			}
			if resp.AuthToken != "ok" {
				return "", fmt.Errorf("握手被拒: %s", resp.AuthToken)
			}
			return resp.NodeID, nil
		}
		return "", err
	}
	if err = c.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return "", err
	}
	if _, err = c.Write(framed); err != nil {
		return "", err
	}
	_ = c.SetWriteDeadline(time.Time{})
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var l int32
	if err = binary.Read(c, binary.BigEndian, &l); err != nil {
		return "", err
	}
	if l <= 0 || l > 1<<10 {
		return "", fmt.Errorf("握手响应长度非法")
	}
	rb := make([]byte, l)
	if _, err = io.ReadFull(c, rb); err != nil {
		return "", err
	}
	_ = c.SetReadDeadline(time.Time{})
	var resp WireFrame
	if err = json.Unmarshal(rb, &resp); err != nil {
		return "", err
	}
	if resp.AuthToken != "ok" {
		return "", fmt.Errorf("握手被拒: %s", resp.AuthToken)
	}
	return resp.NodeID, nil
}

func (n *Node) verifyHandshake(c net.Conn, isWS bool) (string, error) {
	if isWS {
		_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
		_, data, err := c.(*wsConnWrapper).conn.ReadMessage()
		if err != nil {
			return "", err
		}
		if len(data) < 4 {
			return "", fmt.Errorf("握手长度非法")
		}
		hl := binary.BigEndian.Uint32(data[:4])
		if hl == 0 || hl > 1<<10 || int(hl)+4 > len(data) {
			return "", fmt.Errorf("握手长度非法")
		}
		var wf WireFrame
		if err := json.Unmarshal(data[4:4+hl], &wf); err != nil {
			return "", err
		}
		if !util.SecureCompare(wf.AuthToken, n.cfg.Token) {
			_ = n.writeHandshakeWS(c, WireFrame{AuthToken: "unauthorized", NodeID: n.selfID})
			return "", fmt.Errorf("token 不匹配")
		}
		if err := n.writeHandshakeWS(c, WireFrame{AuthToken: "ok", NodeID: n.selfID}); err != nil {
			return "", err
		}
		return wf.NodeID, nil
	}
	_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
	var l int32
	if err := binary.Read(c, binary.BigEndian, &l); err != nil {
		return "", err
	}
	if l <= 0 || l > 1<<10 {
		return "", fmt.Errorf("握手长度非法")
	}
	rb := make([]byte, l)
	if _, err := io.ReadFull(c, rb); err != nil {
		return "", err
	}
	var wf WireFrame
	if err := json.Unmarshal(rb, &wf); err != nil {
		return "", err
	}
	if !util.SecureCompare(wf.AuthToken, n.cfg.Token) {
		fail := WireFrame{AuthToken: "unauthorized", NodeID: n.selfID}
		_ = n.writeHandshakeTCP(c, fail)
		return "", fmt.Errorf("token 不匹配")
	}
	if err := n.writeHandshakeTCP(c, WireFrame{AuthToken: "ok", NodeID: n.selfID}); err != nil {
		return "", err
	}
	return wf.NodeID, nil
}

func (n *Node) writeHandshakeWS(c net.Conn, wf WireFrame) error {
	b, _ := json.Marshal(wf)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := c.(*wsConnWrapper).conn.WriteMessage(websocket.BinaryMessage, append(lb[:], b...))
	_ = c.SetWriteDeadline(time.Time{})
	_ = c.SetReadDeadline(time.Time{})
	return err
}

func (n *Node) writeHandshakeTCP(c net.Conn, wf WireFrame) error {
	b, _ := json.Marshal(wf)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := c.Write(append(lb[:], b...))
	_ = c.SetWriteDeadline(time.Time{})
	_ = c.SetReadDeadline(time.Time{})
	return err
}

type fakeResponseWriter struct {
	conn net.Conn
	br   *bufio.Reader
}

func (f *fakeResponseWriter) Header() http.Header { return http.Header{} }
func (f *fakeResponseWriter) Write(b []byte) (int, error) {
	return f.conn.Write(b)
}
func (f *fakeResponseWriter) WriteHeader(statusCode int) {}
func (f *fakeResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return f.conn, bufio.NewReadWriter(f.br, bufio.NewWriter(f.conn)), nil
}

func isAuthErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "unauthorized") || strings.Contains(s, "token 不匹配") || strings.Contains(s, "握手被拒")
}

func (n *Node) joinLoop() {
	authFails := 0
	for {
		var c net.Conn
		var err error
		if strings.HasPrefix(n.cfg.JoinAddr, "wss://") || strings.HasPrefix(n.cfg.JoinAddr, "ws://") {
			c, _, err = n.dialWSS(n.cfg.JoinAddr)
		} else {
			tc, tcpErr := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", cleanAddr(n.cfg.JoinAddr), n.tlsCfg)
			if tcpErr == nil {
				if _, err = n.handshake(tc, false); err != nil {
					log.Printf("[cluster] TCP Join %s 握手失败: %v", n.cfg.JoinAddr, err)
					_ = tc.Close()
					tcpErr = err
				} else {
					c = tc
				}
			} else {
				c = tc
				err = tcpErr
			}
		}
		if err != nil {
			if isAuthErr(err) {
				authFails++
				shift := authFails - 1
				if shift > 10 {
					shift = 10
				}
				d := 10 * time.Second * time.Duration(1<<uint(shift))
				if d > 3*time.Minute {
					d = 3 * time.Minute
				}
				log.Printf("[cluster] Join %s 鉴权失败, %s后重试 (连续失败 %d 次)", n.cfg.JoinAddr, d, authFails)
				time.Sleep(d)
			} else {
				authFails = 0
				util.LogThrottledf(n.cfg.JoinAddr+"-join", 30*time.Second, "[cluster] Join %s 失败: %v, 3s重试", n.cfg.JoinAddr, err)
				time.Sleep(3 * time.Second)
			}
			continue
		}
		authFails = 0
		log.Printf("[cluster] 节点 %s 已加入 %s (keepalive=%s)", n.selfID, n.cfg.JoinAddr, n.keepAlive)
		setTCPKeepAlive(c, n.keepAlive)
		n.joinMu.Lock()
		n.joinConn = c
		n.joinMu.Unlock()
		isWS := strings.HasPrefix(n.cfg.JoinAddr, "wss://") || strings.HasPrefix(n.cfg.JoinAddr, "ws://")
		tid := "join-" + util.RandomHex(4)
		n.Tunnels.Open(tid, "outbound", "", n.cfg.JoinAddr, c.LocalAddr().String())
		n.handleConn(c, isWS, tid)
		n.Tunnels.Close(tid)
		util.LogDebugf("[cluster] 节点 %s 与 %s 的连接已断开, 3s后重连", n.selfID, n.cfg.JoinAddr)
		n.joinMu.Lock()
		n.joinConn = nil
		n.joinMu.Unlock()
		time.Sleep(3 * time.Second)
	}
}

func peerBackoffDuration(failCount int) time.Duration {
	if failCount <= 0 {
		return 30 * time.Second
	}
	shift := failCount - 1
	if shift > 10 {
		shift = 10
	}
	d := 30 * time.Second * time.Duration(1<<uint(shift))
	if d > time.Hour {
		d = time.Hour
	}
	return d
}

func (n *Node) probeLoop() {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for range t.C {
		n.mu.RLock()
		list := make([]*Peer, 0, len(n.peers))
		for _, p := range n.peers {
			list = append(list, p)
		}
		n.mu.RUnlock()
		for _, p := range list {
			n.mu.RLock()
			until := n.peers[p.ID]
			var skip bool
			if until != nil && !until.UnavailableUntil.IsZero() && time.Now().Before(until.UnavailableUntil) {
				skip = true
			}
			n.mu.RUnlock()
			if skip {
				continue
			}
			start := time.Now()
			dummyURL, _ := url.Parse(PingPath)
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
					peer.FailCount++
					d := peerBackoffDuration(peer.FailCount)
					peer.UnavailableUntil = time.Now().Add(d)
					if err != nil {
						util.LogDebugf("[cluster] ping peer %s fail (tunnel): %v, 标记不可用 %s (%d 次)", p.Addr, err, d, peer.FailCount)
					} else {
						util.LogDebugf("[cluster] ping peer %s status=%d (tunnel), 标记不可用 %s (%d 次)", p.Addr, resp.StatusCode, d, peer.FailCount)
					}
					n.mu.Unlock()
					continue
				}
				peer.FailCount = 0
				peer.UnavailableUntil = time.Time{}
				resp.Body.Close()
				util.LogDebugf("[cluster] ping peer %s ok rtt=%dms (tunnel)", p.Addr, rtt)
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) handleConn(c net.Conn, isWS bool, tunnelID string) {
	defer c.Close()
	defer n.removeWriter(c)
	if tunnelID != "" {
		defer n.Tunnels.Close(tunnelID)
	}
	defer n.failPending(c)
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(n.keepAlive)
	}
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			data := pingFrame()
			log.Printf("[cluster] send ping to %s tunnel=%s", c.RemoteAddr(), tunnelID)
			if err := n.writeConn(c, data); err != nil {
				log.Printf("[cluster] ping failed to %s: %v", c.RemoteAddr(), err)
				return
			}
		}
	}()

	for {
		var head []byte
		var body []byte
		if isWS {
			_ = c.SetReadDeadline(time.Now().Add(util.UpstreamTimeout))
			ws := c.(*wsConnWrapper).conn
			_, data, err := ws.ReadMessage()
			if err != nil {
				return
			}
			if len(data) < 4 {
				continue
			}
			hl := binary.BigEndian.Uint32(data[:4])
			if hl == 0 || hl > 1<<20 || int(hl)+4 > len(data) {
				continue
			}
			head = data[4 : 4+hl]
			body = data[4+hl:]
		} else {
			_ = c.SetReadDeadline(time.Now().Add(util.UpstreamTimeout))
			var l int32
			if err := binary.Read(c, binary.BigEndian, &l); err != nil {
				return
			}
			if l <= 0 || l > 1<<20 {
				return
			}
			head = make([]byte, l)
			if _, err := io.ReadFull(c, head); err != nil {
				return
			}
		}

		var peek WireFrame
		if err := json.Unmarshal(head, &peek); err != nil {
			continue
		}
		if peek.Path == PingPath && peek.StatusCode == nil {
			log.Printf("[cluster] recv ping id=%s from %s tunnel=%s (raw)", peek.ID, c.RemoteAddr(), tunnelID)
		}
		if peek.StatusCode != nil && strings.HasPrefix(peek.ID, "ping-") {
			log.Printf("[cluster] recv pong id=%s from %s tunnel=%s (raw)", peek.ID, c.RemoteAddr(), tunnelID)
		}
		if peek.IsChunk || peek.Done || peek.StatusCode != nil || peek.Path == PingPath {
			n.dispatchFrame(c, isWS, head, body, tunnelID)
			continue
		}
		if peek.BodyLen > 0 && !isWS {
			b := make([]byte, peek.BodyLen)
			if _, err := io.ReadFull(c, b); err != nil {
				return
			}
			body = b
		}
		headCopy := make([]byte, len(head))
		copy(headCopy, head)
		bodyCopy := make([]byte, len(body))
		copy(bodyCopy, body)
		go n.dispatchFrame(c, isWS, headCopy, bodyCopy, tunnelID)
	}
}

func (n *Node) ShouldFailover(status int, isTimeout bool) bool {
	if isTimeout && n.cfg.FailTO {
		return true
	}
	_, ok := n.cfg.FailoverOn[status]
	return ok
}

func (n *Node) PickPeers(visited map[string]bool) []*Peer {
	n.mu.RLock()
	defer n.mu.RUnlock()
	var list []*Peer
	var unavailable []*Peer
	now := time.Now()
	for _, p := range n.peers {
		if visited[p.ID] {
			continue
		}
		if p.NodeID != "" && visited[p.NodeID] {
			continue
		}
		if !p.UnavailableUntil.IsZero() && now.Before(p.UnavailableUntil) {
			unavailable = append(unavailable, p)
			continue
		}
		list = append(list, p)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].RTTms < list[j].RTTms })
	if len(list) == 0 && len(unavailable) > 0 {
		sort.Slice(unavailable, func(i, j int) bool { return unavailable[i].RTTms < unavailable[j].RTTms })
		return unavailable
	}
	return list
}

func (n *Node) ForwardToPeer(ctx context.Context, peer *Peer, orig *http.Request, body []byte, visited []string, hop int) (*http.Response, error) {
	var c net.Conn
	n.joinMu.Lock()
	jc := n.joinConn
	n.joinMu.Unlock()
	if jc != nil && n.cfg.JoinAddr != "" && peer.Addr == n.cfg.JoinAddr {
		c = jc
	} else if v, ok := n.peerConns.Load(peer.Addr); ok {
		if cc, ok := v.(net.Conn); ok && cc != nil {
			c = cc
		}
	} else if v, ok := n.inboundConns.Load(peer.Addr); ok {
		if cc, ok := v.(net.Conn); ok && cc != nil {
			c = cc
		}
	}
	if c == nil {
		n.mu.Lock()
		if p := n.peers[peer.ID]; p != nil {
			p.FailCount++
			p.UnavailableUntil = time.Now().Add(peerBackoffDuration(p.FailCount))
		}
		n.mu.Unlock()
		return nil, fmt.Errorf("no tls tunnel to peer %s", peer.Addr)
	}
	resp, err := n.forwardViaFrame(ctx, c, orig, body, visited, hop)
	n.mu.Lock()
	if p := n.peers[peer.ID]; p != nil {
		if err != nil {
			s := err.Error()
			isTO := strings.Contains(s, "timeout") || strings.Contains(s, "deadline") || strings.Contains(s, "超时")
			if isTO || strings.Contains(s, "tunnel") {
				p.FailCount++
				p.UnavailableUntil = time.Now().Add(peerBackoffDuration(p.FailCount))
			}
		} else {
			p.FailCount = 0
			p.UnavailableUntil = time.Time{}
		}
	}
	n.mu.Unlock()
	return resp, err
}

func (n *Node) ForwardCluster(ctx context.Context, r *http.Request, body []byte) (*http.Response, error) {
	visitedIn, hopIn := ParseVisited(r)
	visitedMap := map[string]bool{}
	for _, v := range visitedIn {
		visitedMap[strings.TrimSpace(v)] = true
	}
	// 已访问过本节点则跳过本节点，直接选下一未访问 peer
	if visitedMap[n.selfID] {
		peers := n.PickPeers(visitedMap)
		var lastErr error
		for _, peer := range peers {
			if hopIn+1 > MaxHop {
				break
			}
			resp, err := n.ForwardToPeer(ctx, peer, r, body, visitedIn, hopIn+1)
			if err == nil {
				return resp, nil
			}
			lastErr = err
		}
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, fmt.Errorf("无可用 peer")
	}
	visited, hop := BuildVisited(n.selfID, visitedIn, hopIn)
	if hop > MaxHop {
		return nil, fmt.Errorf("hop exceeded")
	}
	visitedMap[n.selfID] = true
	peers := n.PickPeers(visitedMap)
	var lastErr error
	for _, peer := range peers {
		if hop+1 > MaxHop {
			break
		}
		resp, err := n.ForwardToPeer(ctx, peer, r, body, visited, hop+1)
		if err == nil {
			return resp, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("无可用 peer")
}

func (n *Node) forwardViaFrame(ctx context.Context, conn net.Conn, orig *http.Request, body []byte, visited []string, hop int) (*http.Response, error) {
	reqID := "r-" + util.RandomHex(8)
	isStream := bytes.Contains(bytes.ToLower(body), []byte(`"stream":true`)) || bytes.Contains(body, []byte(`"stream": true`))
	ch := make(chan pendingResp, 1)
	var streamCh chan pendingResp
	if isStream {
		streamCh = make(chan pendingResp, 64)
	}
	n.pendingMu.Lock()
	n.pending[reqID] = &pendingEntry{ch: ch, conn: conn, streamCh: streamCh, isStream: isStream}
	n.pendingMu.Unlock()
	cleanupPending := func() {
		n.pendingMu.Lock()
		delete(n.pending, reqID)
		n.pendingMu.Unlock()
	}

	wf := WireFrame{
		ID:      reqID,
		Method:  orig.Method,
		Path:    orig.URL.RequestURI(),
		Visited: visited,
		Hop:     hop,
		Egress:  orig.Header.Get(HdrEgress),
		BodyLen: int64(len(body)),
		Stream:  isStream,
		Headers: map[string]string{},
	}
	for k, vv := range orig.Header {
		switch strings.ToLower(k) {
		case strings.ToLower(HdrVisited), strings.ToLower(HdrHop), strings.ToLower(HdrToken), strings.ToLower(HdrEgress):
			continue
		}
		if len(vv) > 0 {
			wf.Headers[k] = vv[0]
		}
	}
	frame, _ := json.Marshal(wf)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(frame)))
	packed := make([]byte, 0, 4+len(frame)+len(body))
	packed = append(packed, lb[:]...)
	packed = append(packed, frame...)
	packed = append(packed, body...)
	if err := n.writeConn(conn, packed); err != nil {
		cleanupPending()
		return nil, err
	}

	timeoutC := time.After(util.UpstreamTimeout)
	select {
	case pr := <-ch:
		if pr.err != nil {
			cleanupPending()
			return nil, pr.err
		}
		if isStream && pr.rf.Header != nil {
			if ct, ok := pr.rf.Header["Content-Type"]; ok {
				for _, v := range ct {
					if strings.Contains(v, "event-stream") {
						select {
						case first := <-streamCh:
							if first.err != nil {
								cleanupPending()
								return nil, first.err
							}
							if first.done {
								cleanupPending()
								return buildStaticResponse(pr), nil
							}
							return buildStreamResponse(pr.rf, streamCh, &first, cleanupPending), nil
						case <-timeoutC:
							cleanupPending()
							return nil, fmt.Errorf("等待对端流式首包超时 (%s)", util.UpstreamTimeout)
						case <-ctx.Done():
							cleanupPending()
							return nil, ctx.Err()
						}
					}
				}
			}
		}
		if isStream && streamCh != nil {
			select {
			case first := <-streamCh:
				if first.isChunk {
					return buildStreamResponse(pr.rf, streamCh, &first, cleanupPending), nil
				}
				if first.done {
					cleanupPending()
					return buildStaticResponse(pr), nil
				}
			default:
			}
		}
		cleanupPending()
		return buildStaticResponse(pr), nil
	case <-timeoutC:
		cleanupPending()
		return nil, fmt.Errorf("等待对端响应超时 (%s)", util.UpstreamTimeout)
	case <-ctx.Done():
		cleanupPending()
		return nil, ctx.Err()
	}
}

func buildStaticResponse(pr pendingResp) *http.Response {
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
	return &http.Response{StatusCode: pr.rf.StatusCode, Header: hdr, Body: io.NopCloser(bytes.NewReader(respBody))}
}

func (n *Node) deliverPending(id string, status *int, hdr map[string][]string, body []byte) {
	n.pendingMu.Lock()
	pe := n.pending[id]
	n.pendingMu.Unlock()
	if pe == nil || pe.ch == nil {
		return
	}
	select {
	case pe.ch <- pendingResp{rf: respFrame{ID: id, StatusCode: *status, Header: hdr, BodyLen: int64(len(body))}, body: body}:
	default:
	}
}

func (n *Node) deliverChunk(id string, chunk []byte, done bool, status *int, hdr map[string][]string) {
	n.pendingMu.Lock()
	pe := n.pending[id]
	n.pendingMu.Unlock()
	if pe == nil {
		return
	}
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
		case pe.ch <- pendingResp{rf: respFrame{ID: id, StatusCode: *status, Header: hdr}}:
		default:
		}
		return
	}
	if done {
		if pe.overflow > 0 {
			errMsg := fmt.Errorf("集群流被截断: 消费侧拥塞, 丢弃 %d 个数据块", pe.overflow)
			select {
			case pe.streamCh <- pendingResp{err: errMsg}:
			default:
			}
			select {
			case pe.ch <- pendingResp{err: errMsg}:
			default:
			}
			return
		}
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
		pe.overflow++
	}
}

func (n *Node) failPending(c net.Conn) {
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

func buildStreamResponse(rf respFrame, ch chan pendingResp, first *pendingResp, onDone func()) *http.Response {
	hdr := http.Header{}
	for k, vv := range rf.Header {
		for _, v := range vv {
			hdr.Add(k, v)
		}
	}
	pr, pw := io.Pipe()
	go func() {
		defer func() {
			if onDone != nil {
				onDone()
			}
		}()
		writeItem := func(item pendingResp) bool {
			if item.err != nil {
				_ = pw.CloseWithError(item.err)
				return false
			}
			if item.done {
				_ = pw.Close()
				return false
			}
			if item.isChunk && len(item.chunk) > 0 {
				if _, err := pw.Write(item.chunk); err != nil {
					return false
				}
			}
			return true
		}
		if first != nil {
			if !writeItem(*first) {
				return
			}
		}
		for item := range ch {
			if !writeItem(item) {
				return
			}
		}
		_ = pw.Close()
	}()
	return &http.Response{StatusCode: rf.StatusCode, Header: hdr, Body: pr}
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
	case PingPath, "/_cluster/peers":
		return true
	}
	return false
}

func ParseVisited(r *http.Request) ([]string, int) {
	visitedStr := r.Header.Get(HdrVisited)
	parts := []string{}
	if visitedStr != "" {
		parts = strings.Split(visitedStr, ",")
	}
	for i := 0; i < len(parts); i++ {
		parts[i] = strings.TrimSpace(parts[i])
	}
	hop := 0
	fmt.Sscanf(r.Header.Get(HdrHop), "%d", &hop)
	return parts, hop
}

func BuildVisited(self string, in []string, hop int) ([]string, int) {
	visited := append([]string(nil), in...)
	visited = append(visited, self)
	return visited, hop + 1
}

func handleClusterHTTP(w http.ResponseWriter, r *http.Request, node *Node) bool {
	if node == nil || !node.Enabled() {
		return false
	}
	switch r.URL.Path {
	case "/_cluster/peers":
		node.mu.RLock()
		list := make([]*Peer, 0, len(node.peers))
		for _, p := range node.peers {
			list = append(list, p)
		}
		node.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(list)
		return true
	}
	return false
}

func pingFrame() []byte {
	wf := WireFrame{
		ID:   "ping-" + util.RandomHex(4),
		Path: PingPath,
	}
	b, _ := json.Marshal(wf)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
	out := make([]byte, 0, 4+len(b))
	out = append(out, lb[:]...)
	out = append(out, b...)
	return out
}
