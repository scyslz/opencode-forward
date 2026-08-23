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

	"github.com/gorilla/websocket"
)

const (
	clusterForwardPath = "/_cluster/forward"
	clusterPingPath    = "/_cluster/ping"
	clusterPeerUpdatePath = "/_cluster/peer_update"
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
	WSSAddr string `json:"wss_addr,omitempty"`
	RTTms   int64  `json:"rtt_ms"`
	Dynamic bool   `json:"-"`
}

type clusterConfig struct {
	ID          string
	Token       string
	ListenAddr  string
	JoinAddr    string
	JoinWSSAddr string
	Peers       []*clusterPeer
	FailoverOn  map[int]bool
	FailTO      bool
	TunnelFile  string
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
	ch        chan pendingResp
	conn      net.Conn
	streamCh  chan pendingResp
	isStream  bool
	overflow  int
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
	AuthToken  string               `json:"auth_token,omitempty"`
}

type connWriter struct {
	conn net.Conn
	ch   chan []byte
	done chan struct{}
	mu   sync.Mutex
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
	connWriters  sync.Map
	pendingMu    sync.Mutex
	pending      map[string]*pendingEntry
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
				parts := strings.Split(args[i], ",")
				cfg.JoinAddr = parts[0]
				if len(parts) > 1 {
					cfg.JoinWSSAddr = parts[1]
				}
			}
		case "--peer":
			if i+1 < len(args) {
				i++
				parts := strings.Split(args[i], ",")
				peer := &clusterPeer{
					ID:   parts[0],
					Addr: parts[0],
				}
				if len(parts) > 1 {
					peer.WSSAddr = parts[1]
				}
				cfg.Peers = append(cfg.Peers, peer)
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
		n.peers[p.ID] = p
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

func (n *clusterNode) getWriter(c net.Conn) *connWriter {
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

func (n *clusterNode) removeWriter(c net.Conn) {
	key := fmt.Sprintf("%p", c)
	if v, ok := n.connWriters.Load(key); ok {
		cw := v.(*connWriter)
		n.connWriters.Delete(key)
		close(cw.ch)
	}
}

func (n *clusterNode) writeConn(c net.Conn, data []byte) error {
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

func packFrame(wf clusterWireFrame, body []byte) []byte {
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

func (n *clusterNode) loopPeerTunnel(p *clusterPeer) {
	for {
		var c net.Conn
		var err error

		// 自动判断 TCP 还是 WSS
		if strings.HasPrefix(p.Addr, "wss://") || strings.HasPrefix(p.Addr, "ws://") {
			c, err = n.dialWSS(p.Addr)
		} else {
			c, err = tls.Dial("tcp", p.Addr, n.tlsCfg)
			if err == nil {
				if err = n.handshake(c, false); err != nil {
					log.Printf("[cluster] TCP 握手 peer %s 失败: %v", p.Addr, err)
					_ = c.Close()
					time.Sleep(3 * time.Second)
					continue
				}
			}
		}

		if err != nil {
			log.Printf("[cluster] 拨号 peer %s 失败: %v, 3s重试", p.Addr, err)
			time.Sleep(3 * time.Second)
			continue
		}

		setTCPKeepAlive(c, n.keepAlive)
		n.peerConns.Store(p.ID, c)
		log.Printf("[cluster] peer %s 隧道已建立 (keepalive=%s)", p.Addr, n.keepAlive)
		isWS := strings.HasPrefix(p.Addr, "wss://") || strings.HasPrefix(p.Addr, "ws://")
		tid := "out-" + randomHex(4)
		n.tunnels.open(tid, "outbound", "", p.Addr, c.LocalAddr().String())
		n.handleConn(c, isWS, tid)
		n.tunnels.close(tid)
		n.peerConns.Delete(p.ID)
		_ = c.Close()
		time.Sleep(3 * time.Second)
	}
}

func (n *clusterNode) dialWSS(addr string) (net.Conn, error) {
	dialer := websocket.Dialer{
		TLSClientConfig: n.tlsCfg,
	}
	ws, _, err := dialer.Dial(addr, nil)
	if err != nil {
		return nil, err
	}
	c := &wsConnWrapper{conn: ws}
	if err := n.handshake(c, true); err != nil {
		_ = c.Close()
		return nil, err
	}
	return c, nil
}

	var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (n *clusterNode) acceptLoop() {
	for {
		c, err := n.listener.Accept()
		if err != nil {
			return
		}

		// 简单的 HTTP/WS 握手嗅探
		br := bufio.NewReader(c)
		peek, _ := br.Peek(4)
		if len(peek) >= 3 && string(peek[:3]) == "GET" {
			go func(c net.Conn, br *bufio.Reader) {
				req, _ := http.ReadRequest(br)
				if req == nil {
					c.Close()
					return
				}
				ws, err := wsUpgrader.Upgrade(&fakeResponseWriter{conn: c, br: br}, req, nil)
				if err != nil {
					log.Printf("[cluster] WS 升级失败: %v", err)
					c.Close()
					return
				}
				conn := &wsConnWrapper{conn: ws}
				if err := n.verifyHandshake(conn, true); err != nil {
					log.Printf("[cluster] WSS 握手被拒: %v", err)
					_ = conn.Close()
					return
				}
				n.handleConn(conn, true, "ws-"+randomHex(4))
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
			log.Printf("[cluster] 隧道下线剔除: %s", conn.RemoteAddr())
		})
	}
}

func (n *clusterNode) handleRawConn(c net.Conn, onDone func(net.Conn, string)) {
	remote := c.RemoteAddr().String()
	local := c.LocalAddr().String()
	underlying := c
	if tcw, ok := c.(*tcpConnWrapper); ok {
		underlying = tcw.Conn
	}
	if tlsConn, ok := underlying.(*tls.Conn); ok {
		if hErr := tlsConn.Handshake(); hErr != nil {
			log.Printf("[cluster] 接受连接 TLS握手失败: %v", hErr)
			_ = c.Close()
			return
		}
	}
	if err := n.verifyHandshake(c, false); err != nil {
		log.Printf("[cluster] 匿名裸TCP 连接被拒绝: %v (from %s)", err, remote)
		_ = c.Close()
		return
	}
	tid := "in-" + randomHex(4)
	n.tunnels.open(tid, "inbound", "", remote, local)
	log.Printf("[cluster] 接受隧道连接: id=%s remote=%s local=%s (服务端日志)", tid, remote, local)
	n.mu.Lock()
	n.peers[remote] = &clusterPeer{ID: remote, Addr: remote, RTTms: 9999, Dynamic: true}
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
func (w *wsConnWrapper) SetDeadline(t time.Time) error      { return nil }
func (w *wsConnWrapper) SetReadDeadline(t time.Time) error  { return nil }
func (w *wsConnWrapper) SetWriteDeadline(t time.Time) error { return nil }

type tcpConnWrapper struct {
	net.Conn
	io.Reader
}

func (t *tcpConnWrapper) Read(b []byte) (int, error) { return t.Reader.Read(b) }

func (n *clusterNode) handshake(c net.Conn, isWS bool) error {
	frame := clusterWireFrame{AuthToken: n.cfg.Token}
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
				return rerr
			}
			if len(data) < 4 {
				return fmt.Errorf("握手响应长度非法")
			}
			hl := binary.BigEndian.Uint32(data[:4])
			if hl == 0 || hl > 1<<10 || int(hl)+4 > len(data) {
				return fmt.Errorf("握手响应长度非法")
			}
			var resp clusterWireFrame
			if err2 := json.Unmarshal(data[4:4+hl], &resp); err2 != nil {
				return err2
			}
			if resp.AuthToken != "ok" {
				return fmt.Errorf("握手被拒: %s", resp.AuthToken)
			}
			return nil
		}
		return err
	}
	if err = c.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	if _, err = c.Write(framed); err != nil {
		return err
	}
	_ = c.SetWriteDeadline(time.Time{})
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var l int32
	if err = binary.Read(c, binary.BigEndian, &l); err != nil {
		return err
	}
	if l <= 0 || l > 1<<10 {
		return fmt.Errorf("握手响应长度非法")
	}
	rb := make([]byte, l)
	if _, err = io.ReadFull(c, rb); err != nil {
		return err
	}
	_ = c.SetReadDeadline(time.Time{})
	var resp clusterWireFrame
	if err = json.Unmarshal(rb, &resp); err != nil {
		return err
	}
	if resp.AuthToken != "ok" {
		return fmt.Errorf("握手被拒: %s", resp.AuthToken)
	}
	return nil
}

func (n *clusterNode) verifyHandshake(c net.Conn, isWS bool) error {
	if isWS {
		_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
		_, data, err := c.(*wsConnWrapper).conn.ReadMessage()
		if err != nil {
			return err
		}
		if len(data) < 4 {
			return fmt.Errorf("握手长度非法")
		}
		hl := binary.BigEndian.Uint32(data[:4])
		if hl == 0 || hl > 1<<10 || int(hl)+4 > len(data) {
			return fmt.Errorf("握手长度非法")
		}
		var wf clusterWireFrame
		if err := json.Unmarshal(data[4:4+hl], &wf); err != nil {
			return err
		}
		if !secureCompare(wf.AuthToken, n.cfg.Token) {
			_ = n.writeHandshakeWS(c, clusterWireFrame{AuthToken: "unauthorized"})
			return fmt.Errorf("token 不匹配")
		}
		return n.writeHandshakeWS(c, clusterWireFrame{AuthToken: "ok"})
	}
	_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
	var l int32
	if err := binary.Read(c, binary.BigEndian, &l); err != nil {
		return err
	}
	if l <= 0 || l > 1<<10 {
		return fmt.Errorf("握手长度非法")
	}
	rb := make([]byte, l)
	if _, err := io.ReadFull(c, rb); err != nil {
		return err
	}
	var wf clusterWireFrame
	if err := json.Unmarshal(rb, &wf); err != nil {
		return err
	}
	if !secureCompare(wf.AuthToken, n.cfg.Token) {
		fail := clusterWireFrame{AuthToken: "unauthorized"}
		_ = n.writeHandshakeTCP(c, fail)
		return fmt.Errorf("token 不匹配")
	}
	return n.writeHandshakeTCP(c, clusterWireFrame{AuthToken: "ok"})
}

func (n *clusterNode) writeHandshakeWS(c net.Conn, wf clusterWireFrame) error {
	b, _ := json.Marshal(wf)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := c.(*wsConnWrapper).conn.WriteMessage(websocket.BinaryMessage, append(lb[:], b...))
	_ = c.SetWriteDeadline(time.Time{})
	_ = c.SetReadDeadline(time.Time{})
	return err
}

func (n *clusterNode) writeHandshakeTCP(c net.Conn, wf clusterWireFrame) error {
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

func (n *clusterNode) joinLoop() {
	for {
		var c net.Conn
		var err error

		// 根据前缀自动判断 TCP/WSS
		if strings.HasPrefix(n.cfg.JoinAddr, "wss://") || strings.HasPrefix(n.cfg.JoinAddr, "ws://") {
			c, err = n.dialWSS(n.cfg.JoinAddr)
		} else {
			tc, tcpErr := tls.Dial("tcp", cleanAddr(n.cfg.JoinAddr), n.tlsCfg)
			if tcpErr == nil {
				if err = n.handshake(tc, false); err != nil {
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
			log.Printf("[cluster] Join %s 失败: %v, 3s重试", n.cfg.JoinAddr, err)
			time.Sleep(3 * time.Second)
			continue
		}

		log.Printf("[cluster] 节点 %s 与 %s 握手成功", n.selfID, n.cfg.JoinAddr)
		setTCPKeepAlive(c, n.keepAlive)
		log.Printf("[cluster] 节点 %s 已加入 %s (keepalive=%s)", n.selfID, n.cfg.JoinAddr, n.keepAlive)
		n.joinMu.Lock()
		n.joinConn = c
		n.joinMu.Unlock()
		isWS := strings.HasPrefix(n.cfg.JoinAddr, "wss://") || strings.HasPrefix(n.cfg.JoinAddr, "ws://")
		tid := "join-" + randomHex(4)
		n.tunnels.open(tid, "outbound", "", n.cfg.JoinAddr, c.LocalAddr().String())
		n.handleConn(c, isWS, tid)
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

func (n *clusterNode) handleConn(c net.Conn, isWS bool, tunnelID string) {
	defer c.Close()
	defer n.removeWriter(c)
	if tunnelID != "" {
		defer n.tunnels.close(tunnelID)
	}
	defer n.failPending(c)
	if tcp, ok := c.(*net.TCPConn); ok {
		_ = tcp.SetKeepAlive(true)
		_ = tcp.SetKeepAlivePeriod(n.keepAlive)
	}
	// 定期发送 ping 保持连接活跃（有活跃请求时跳过）
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			n.pendingMu.Lock()
			active := false
			for _, pe := range n.pending {
				if pe.conn == c {
					active = true
					break
				}
			}
			n.pendingMu.Unlock()
			if active {
				log.Printf("[cluster] skip ping: %d in-flight", len(n.pending))
				continue
			}
			if err := n.writeConn(c, pingFrame()); err != nil {
				log.Printf("[cluster] ping failed: %v", err)
				break
			}
		}
	}()

	for {
		// 读取一个完整 Frame: [4字节长度][JSON head][body]
		var head []byte
		var body []byte
		if isWS {
			// WebSocket 模式：一个 Message 就是一个完整 Frame
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
			// TCP 模式：从流中按长度解析
			_ = c.SetReadDeadline(time.Now().Add(2 * n.keepAlive))
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

		var wf clusterWireFrame
		if err := json.Unmarshal(head, &wf); err != nil {
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
			if wf.BodyLen > 0 && !isWS {
				// TCP 模式需要从流中读取 body
				b := make([]byte, wf.BodyLen)
				if _, err := io.ReadFull(c, b); err != nil {
					return
				}
				body = b
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
			if wf.StatusCode == nil {
				respBody := []byte("pong")
				rf := clusterRespFrame{ID: wf.ID, StatusCode: 200, Header: map[string][]string{}, BodyLen: int64(len(respBody))}
				rfb, _ := json.Marshal(rf)
				var rlb [4]byte
				binary.BigEndian.PutUint32(rlb[:], uint32(len(rfb)))
				packed := make([]byte, 0, 4+len(rfb)+len(respBody))
				packed = append(packed, rlb[:]...)
				packed = append(packed, rfb...)
				packed = append(packed, respBody...)
				_ = n.writeConn(c, packed)
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
		fwdReq.Header.Set(clusterHdrVisited, strings.Join(h.Visited, ","))
		fwdReq.Header.Set(clusterHdrHop, fmt.Sprintf("%d", h.Hop))
		fwdReq.Header.Set(clusterHdrToken, n.cfg.Token)
		fwdReq.Header.Set(clusterHdrEgress, h.Egress)
		var reqBody []byte
		if h.BodyLen > 0 {
			if isWS {
				reqBody = body
			} else {
				reqBody = make([]byte, h.BodyLen)
				if _, err := io.ReadFull(c, reqBody); err != nil {
					continue
				}
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
			initFrame := clusterWireFrame{ID: h.ID, StatusCode: intPtr(200), Header: hdrs, IsChunk: false, Stream: true}
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
					cf := clusterWireFrame{ID: h.ID, IsChunk: true, Chunk: chunk}
					if werr := n.writeConn(c, packFrame(cf, nil)); werr != nil {
						break
					}
				}
				if rerr != nil {
					break
				}
			}
			resp.Body.Close()
			doneFrame := clusterWireFrame{ID: h.ID, Done: true}
			_ = n.writeConn(c, packFrame(doneFrame, nil))
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

func (n *clusterNode) forwardViaFrame(ctx context.Context, conn net.Conn, orig *http.Request, body []byte, visited []string, hop int) (*http.Response, error) {
	reqID := "r-" + randomHex(8)
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
	packed := make([]byte, 0, 4+len(frame)+len(body))
	packed = append(packed, lb[:]...)
	packed = append(packed, frame...)
	packed = append(packed, body...)
	if err := n.writeConn(conn, packed); err != nil {
		cleanupPending()
		return nil, err
	}

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

func (n *clusterNode) deliverPending(id string, status *int, hdr map[string][]string, body []byte) {
	n.pendingMu.Lock()
	pe := n.pending[id]
	n.pendingMu.Unlock()
	if pe == nil || pe.ch == nil {
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
		case pe.ch <- pendingResp{rf: clusterRespFrame{ID: id, StatusCode: *status, Header: hdr}}:
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

func buildStreamResponse(rf clusterRespFrame, ch chan pendingResp, first *pendingResp, onDone func()) *http.Response {
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

func pingFrame() []byte {
	wf := clusterWireFrame{
		ID:   "ping-" + randomHex(4),
		Path: clusterPingPath,
	}
	b, _ := json.Marshal(wf)
	var lb [4]byte
	binary.BigEndian.PutUint32(lb[:], uint32(len(b)))
	out := make([]byte, 0, 4+len(b))
	out = append(out, lb[:]...)
	out = append(out, b...)
	return out
}
