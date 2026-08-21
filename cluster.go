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

type clusterPeer struct {
	ID    string `json:"id"`
	Addr  string `json:"addr"`
	RTTms int64 `json:"rtt_ms"`
}

type clusterConfig struct {
	ID         string
	Token      string
	ListenAddr string
	JoinAddr   string
	Peers      []string
	FailoverOn map[int]bool
	FailTO     bool
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
	peerConns sync.Map
	onForward func(r *http.Request) (*http.Response, error)
	keepAlive time.Duration
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
		cfg:    cfg,
		selfID: cfg.ID,
		peers:  map[string]*clusterPeer{},
		tlsCfg: tlsCfg,
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
		n.handleConn(c)
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
		if tlsConn, ok := c.(*tls.Conn); ok {
			if hErr := tlsConn.Handshake(); hErr != nil {
				log.Printf("[cluster] 接受连接 TLS握手失败: %v", hErr)
				_ = c.Close()
				continue
			}
		}
		go n.handleConn(c)
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
		n.handleConn(c)
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
			client := &http.Client{Timeout: 3 * time.Second}
			req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+p.Addr+clusterPingPath, nil)
			if n.cfg.Token != "" {
				req.Header.Set(clusterHdrToken, n.cfg.Token)
			}
			resp, err := client.Do(req)
			rtt := time.Since(start).Milliseconds()
			n.mu.Lock()
			peer := n.peers[p.ID]
			if peer != nil {
				peer.RTTms = rtt
				if err != nil || resp.StatusCode != http.StatusOK {
					if resp != nil {
						resp.Body.Close()
					}
					if err != nil {
						log.Printf("[cluster] ping peer %s fail: %v", p.Addr, err)
					} else {
						resp.Body.Close()
						log.Printf("[cluster] ping peer %s status=%d", p.Addr, resp.StatusCode)
					}
					n.mu.Unlock()
					continue
				}
				resp.Body.Close()
			}
			n.mu.Unlock()
		}
	}
}

func (n *clusterNode) handleConn(c net.Conn) {
	defer c.Close()
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
		var h clusterFrameHeader
		if err := json.Unmarshal(buf, &h); err != nil {
			continue
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
		if n.onForward == nil {
			continue
		}
		bodyR := bytes.NewReader(make([]byte, h.BodyLen))
		fwdReq.Body = io.NopCloser(bodyR)
		fwdReq.ContentLength = h.BodyLen
		resp, err := n.onForward(fwdReq)
		if err != nil {
			continue
		}
		resp.Body.Close()
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
	return nil, fmt.Errorf("no tls tunnel to peer %s", peer.Addr)
}

func (n *clusterNode) forwardViaFrame(ctx context.Context, conn net.Conn, orig *http.Request, body []byte, visited []string, hop int) (*http.Response, error) {
	fh := clusterFrameHeader{
		ID:      n.selfID,
		Method:  orig.Method,
		Path:    orig.URL.RequestURI(),
		Visited: visited,
		Hop:     hop,
		Egress:  orig.Header.Get(clusterHdrEgress),
		BodyLen: int64(len(body)),
	}
	if fh.Headers == nil {
		fh.Headers = map[string]string{}
	}
	for k, vv := range orig.Header {
		switch strings.ToLower(k) {
		case strings.ToLower(clusterHdrVisited), strings.ToLower(clusterHdrHop), strings.ToLower(clusterHdrToken), strings.ToLower(clusterHdrEgress):
			continue
		}
		if len(vv) > 0 {
			fh.Headers[k] = vv[0]
		}
	}
	frame, _ := json.Marshal(fh)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(frame)))
	if _, err := conn.Write(lb[:]); err != nil {
		return nil, err
	}
	if _, err := conn.Write(frame); err != nil {
		return nil, err
	}
	if len(body) > 0 {
		if _, err := conn.Write(body); err != nil {
			return nil, err
		}
	}
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(nil))}, nil
}

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
