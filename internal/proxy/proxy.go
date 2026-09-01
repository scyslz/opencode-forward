package proxy

import (
	"bytes"
	"opencode-zen-proxy/internal/cluster"
	"opencode-zen-proxy/internal/egress"
	"opencode-zen-proxy/internal/util"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)


type SessionCache struct {
	mu   sync.Mutex
	path string
	m    map[string]map[string]string
}

func NewSessionCache(path string) *SessionCache {
	c := &SessionCache{path: path, m: map[string]map[string]string{}}
	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			_ = json.Unmarshal(data, &c.m)
		}
		if c.m == nil {
			c.m = map[string]map[string]string{}
		}
	}
	return c
}

func (c *SessionCache) resolve(nsKey, incoming string) string {
	if incoming == "" {
		return util.OpencodeID("ses")
	}
	if c.path != "" {
		return c.resolvePersist(nsKey, incoming)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	fam := c.m[nsKey]
	if fam == nil {
		fam = map[string]string{}
		c.m[nsKey] = fam
	}
	if v, ok := fam[incoming]; ok {
		return v
	}
	v := util.OpencodeID("ses")
	fam[incoming] = v
	return v
}

func (c *SessionCache) resolvePersist(nsKey, incoming string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.path != "" {
		if data, err := os.ReadFile(c.path); err == nil {
			var disk map[string]map[string]string
			if json.Unmarshal(data, &disk) == nil && disk != nil {
				c.m = disk
			}
		}
	}
	fam := c.m[nsKey]
	if fam == nil {
		fam = map[string]string{}
		c.m[nsKey] = fam
	}
	if v, ok := fam[incoming]; ok {
		return v
	}
	v := util.OpencodeID("ses")
	fam[incoming] = v
	if c.path != "" {
		data, err := json.MarshalIndent(c.m, "", "  ")
		if err == nil {
			dir := filepath.Dir(c.path)
			if dir != "" {
				_ = os.MkdirAll(dir, 0o755)
			}
			tmp := c.path + ".tmp"
			if os.WriteFile(tmp, data, 0o644) == nil {
				_ = os.Rename(tmp, c.path)
			}
		}
	}
	return v
}

func authSummary(authToken string) string {
	if authToken != "" {
		return "Bearer " + authToken
	}
	return "透传客户端Authorization"
}

func isStreamRequest(body []byte) bool {
	if bytes.Contains(body, []byte(`"stream":true`)) || bytes.Contains(body, []byte(`"stream": true`)) || bytes.Contains(body, []byte(`"stream":1`)) {
		return true
	}
	return false
}

func isLocalRetryStatus(code int) bool {
	switch code {
	case 403, 429, 502, 503, 504:
		return true
	}
	return false
}

func (p *Proxy) shouldRetryLocal(code int) bool {
	if p.cluster != nil && p.cluster.Enabled() {
		return p.cluster.ShouldFailover(code, false)
	}
	return isLocalRetryStatus(code)
}

func dumpRequest(r *http.Request, withBody bool) {
	var b strings.Builder
	b.WriteString("\n----- 入站原始请求 -----\n")
	fmt.Fprintf(&b, "%s %s HTTP/%d.%d\n", r.Method, r.URL.RequestURI(), r.ProtoMajor, r.ProtoMinor)
	if r.Host != "" {
		b.WriteString("Host: " + r.Host + "\n")
	}
	keys := make([]string, 0, len(r.Header))
	for k := range r.Header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, v := range r.Header[k] {
			fmt.Fprintf(&b, "%s: %s\n", k, v)
		}
	}
	if withBody && r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		if len(body) > 0 {
			b.WriteString("\nBODY:\n" + string(body) + "\n")
		}
	} else if !withBody && r.Body != nil {
		// 仍需 peek 长度以提示但不打印内容，避免 Body 被消费
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		if len(body) > 0 {
			fmt.Fprintf(&b, "\n[BODY %d bytes, 需 VERBOSE=1 查看详情]\n", len(body))
		}
	}
	b.WriteString("----- 入站 end -----")
	log.Print(b.String())
}

func dumpOutbound(r *http.Request, body []byte, nsKey, inSess, outSess string, withBody bool) {
	var b strings.Builder
	b.WriteString("\n----- 转发真实请求 -----\n")
	if nsKey != "" {
		fmt.Fprintf(&b, "命名空间: %s  会话: %q -> %q\n", nsKey, inSess, outSess)
	}
	uri := r.URL.Path
	if r.URL.RawQuery != "" {
		uri += "?" + r.URL.RawQuery
	}
	fmt.Fprintf(&b, "%s %s://%s%s HTTP/1.1\n", r.Method, r.URL.Scheme, r.URL.Host, uri)
	if r.Host != "" {
		b.WriteString("Host: " + r.Host + "\n")
	}
	keys := make([]string, 0, len(r.Header))
	for k := range r.Header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, v := range r.Header[k] {
			fmt.Fprintf(&b, "%s: %s\n", k, v)
		}
	}
	if withBody && len(body) > 0 {
		b.WriteString("\nBODY:\n" + string(body) + "\n")
	} else if !withBody && len(body) > 0 {
		fmt.Fprintf(&b, "\n[BODY %d bytes, 需 VERBOSE=1 查看详情]\n", len(body))
	}
	b.WriteString("----- 转发 end -----")
	log.Print(b.String())
}

func dumpResponse(resp *http.Response, prefix string, withBody bool) {
	if resp == nil {
		log.Printf("%s <nil>", prefix)
		return
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "event-stream") {
		var b strings.Builder
		fmt.Fprintf(&b, "\n----- %s -----\n", prefix)
		fmt.Fprintf(&b, "HTTP %d %s\n", resp.StatusCode, resp.Status)
		keys := make([]string, 0, len(resp.Header))
		for k := range resp.Header {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			for _, v := range resp.Header[k] {
				fmt.Fprintf(&b, "%s: %s\n", k, v)
			}
		}
		b.WriteString("\n[BODY streaming event-stream, not buffered]\n")
		b.WriteString(fmt.Sprintf("----- %s end -----", prefix))
		log.Print(b.String())
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "\n----- %s -----\n", prefix)
	fmt.Fprintf(&b, "HTTP %d %s\n", resp.StatusCode, resp.Status)
	keys := make([]string, 0, len(resp.Header))
	for k := range resp.Header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, v := range resp.Header[k] {
			fmt.Fprintf(&b, "%s: %s\n", k, v)
		}
	}
	if withBody && resp.Body != nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body = io.NopCloser(bytes.NewReader(body))
		if len(body) > 0 {
			if len(body) > 4096 {
				b.WriteString(fmt.Sprintf("\nBODY [%d bytes, truncated]:\n", len(body)))
				b.WriteString(string(body[:4096]) + "\n...[truncated]\n")
			} else {
				b.WriteString("\nBODY:\n" + string(body) + "\n")
			}
		}
	} else if resp.Body != nil {
		body, _ := io.ReadAll(resp.Body)
		resp.Body = io.NopCloser(bytes.NewReader(body))
		if len(body) > 0 {
			fmt.Fprintf(&b, "\n[BODY %d bytes, 需 VERBOSE=1 查看详情]\n", len(body))
		}
	}
	b.WriteString(fmt.Sprintf("----- %s end -----", prefix))
	log.Print(b.String())
}

type Config struct {
	Scheme       string
	Host         string
	BasePath     string
	BackendURL   *url.URL
	AuthToken    string
	InboundAuth  string
	FwdInbound   bool
	Xff          bool
	Verbose      bool
	Dump         bool
	RewriteModel string
	ExtraHeaders []string
	Defaults     map[string]string
}

type Proxy struct {
	cfg     Config
	egress  *egress.Manager
	sess    *SessionCache
	cluster ClusterForwarder
}

type ClusterForwarder interface {
	Enabled() bool
	ShouldFailover(status int, isTimeout bool) bool
	PickPeers(visited map[string]bool) []*cluster.Peer
	ForwardToPeer(ctx context.Context, peer *cluster.Peer, orig *http.Request, body []byte, visited []string, hop int) (*http.Response, error)
	ForwardCluster(ctx context.Context, r *http.Request, body []byte) (*http.Response, error)
	HandleHTTP(w http.ResponseWriter, r *http.Request) bool
	IsInternalPath(p string) bool
	SelfID() string
}

func New(cfg Config, egress *egress.Manager, sess *SessionCache) *Proxy {
	return &Proxy{cfg: cfg, egress: egress, sess: sess}
}

func (p *Proxy) SetCluster(c ClusterForwarder) {
	p.cluster = c
	if cn, ok := c.(*cluster.Node); ok && cn != nil {
		cn.SetForwarder(p.HandleClusterForward)
	}
}

func isHopByHop(k string) bool {
	switch strings.ToLower(k) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "proxy-connection":
		return true
	}
	return false
}

// passthroughHeader 白名单: 放行 opencode CLI 真实会发送的头 + 客户端认证头,
// hop-by-hop 与其余任意客户端头不透传 (默认 outbound 未配时依赖此透传客户端 Authorization)
func passthroughHeader(k string) bool {
	if isHopByHop(k) {
		return false
	}
	switch strings.ToLower(k) {
	case "accept", "content-type", "accept-encoding", "content-length",
		"authorization", "user-agent",
		"x-opencode-client", "x-opencode-project", "x-opencode-session", "x-opencode-request":
		return true
	}
	return false
}

func normalizeProxyPath(p string) string {
	// 兼容单复数: /response -> /responses, 同时保留子路径如 /responses/xxx
	if p == "/response" {
		return "/responses"
	}
	if strings.HasPrefix(p, "/response/") {
		return "/responses/" + strings.TrimPrefix(p, "/response/")
	}
	if p == "/v1/response" {
		return "/v1/responses"
	}
	if strings.HasPrefix(p, "/v1/response/") {
		return "/v1/responses/" + strings.TrimPrefix(p, "/v1/response/")
	}
	return p
}

func joinPathDedup(base, p string) string {
	p = normalizeProxyPath(p)
	joined := util.JoinPath(base, p)
	if strings.HasSuffix(strings.TrimSuffix(base, "/"), "/v1") {
		joined = strings.ReplaceAll(joined, "/v1/v1/", "/v1/")
		if strings.HasSuffix(joined, "/v1/v1") {
			joined = strings.TrimSuffix(joined, "/v1/v1") + "/v1"
		}
	}
	return joined
}

func (p *Proxy) buildOutbound(in *http.Request, outSession string) *http.Request {
	u := &url.URL{Scheme: p.cfg.Scheme, Host: p.cfg.Host, Path: joinPathDedup(p.cfg.BasePath, in.URL.Path), RawQuery: in.URL.RawQuery}
	out := &http.Request{
		Method: in.Method,
		URL:    u,
		Header: http.Header{},
		Host:   p.cfg.Host,
	}
	for k, vv := range in.Header {
		if strings.EqualFold(k, "X-Egress") || strings.EqualFold(k, cluster.HdrVisited) || strings.EqualFold(k, cluster.HdrHop) || strings.EqualFold(k, cluster.HdrToken) {
			continue
		}
		if !passthroughHeader(k) {
			continue
		}
		for _, v := range vv {
			out.Header.Add(k, v)
		}
	}
	for k, v := range p.cfg.Defaults {
		out.Header.Set(k, v)
	}
	out.Header.Set("X-Opencode-Session", outSession)
	out.Header.Set("X-Opencode-Request", util.OpencodeID("msg"))
	if p.cfg.Xff {
		if clientIP, _, err := net.SplitHostPort(in.RemoteAddr); err == nil {
			out.Header.Set("X-Forwarded-For", clientIP)
		}
	}
	if p.cfg.AuthToken != "" {
		tok := p.cfg.AuthToken
		if !strings.HasPrefix(strings.ToLower(tok), "bearer ") {
			tok = "Bearer " + tok
		}
		out.Header.Set("Authorization", tok)
	} else if p.cfg.FwdInbound && p.cfg.InboundAuth != "" {
		tok := p.cfg.InboundAuth
		if !strings.HasPrefix(strings.ToLower(tok), "bearer ") {
			tok = "Bearer " + tok
		}
		out.Header.Set("Authorization", tok)
	}
	for _, h := range p.cfg.ExtraHeaders {
		k, v, ok := strings.Cut(h, ":")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		if strings.EqualFold(k, "Host") {
			out.Host = v
			continue
		}
		out.Header.Set(k, v)
	}
	return out
}

func (p *Proxy) doLocalBuffered(ctx context.Context, fam string, in *http.Request, body []byte) (*http.Response, error) {
	ns := p.egress.NsKey(fam)
	incomingSession := in.Header.Get("X-Opencode-Session")
	outSession := p.sess.resolve(ns, incomingSession)
	outReq := p.buildOutbound(in, outSession)
	if len(body) > 0 {
		outReq.Body = io.NopCloser(bytes.NewReader(body))
		outReq.ContentLength = int64(len(body))
	}
	if p.cfg.Dump {
		dumpOutbound(outReq, body, ns, incomingSession, outSession, p.cfg.Verbose)
	}
	outReq = outReq.WithContext(ctx)
	tr := p.egress.Transport(fam)
	resp, err := tr.RoundTrip(outReq)
	if err != nil {
		return nil, err
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "event-stream") {
		if p.cfg.Dump {
			dumpResponse(resp, fmt.Sprintf("本地 IPv%s 响应 %s", fam, in.URL.Path), false)
		}
		if p.cfg.Verbose {
			log.Printf("[opencode-proxy] %s %s%s -> IPv%s session:%q->%q status=%d stream=false ct=%q cl=%d (event-stream直通)", in.Method, p.cfg.BackendURL, in.URL.Path, fam, incomingSession, outSession, resp.StatusCode, resp.Header.Get("Content-Type"), resp.ContentLength)
		}
		return resp, nil
	}
	if p.cfg.Dump {
		dumpResponse(resp, fmt.Sprintf("本地 IPv%s 响应 %s", fam, in.URL.Path), p.cfg.Verbose)
	}
	if resp.Body != nil {
		nb, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if rerr != nil {
			return nil, rerr
		}
		resp.Body = io.NopCloser(bytes.NewReader(nb))
		resp.ContentLength = int64(len(nb))
		resp.Header.Del("Transfer-Encoding")
		resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(nb)))
	}
	if p.cfg.Verbose {
		log.Printf("[opencode-proxy] %s %s%s -> IPv%s session:%q->%q status=%d stream=false ct=%q cl=%d", in.Method, p.cfg.BackendURL, in.URL.Path, fam, incomingSession, outSession, resp.StatusCode, resp.Header.Get("Content-Type"), resp.ContentLength)
	}
	return resp, nil
}

func (p *Proxy) doLocalStream(ctx context.Context, fam string, in *http.Request, body []byte) (*http.Response, error) {
	ns := p.egress.NsKey(fam)
	incomingSession := in.Header.Get("X-Opencode-Session")
	outSession := p.sess.resolve(ns, incomingSession)
	outReq := p.buildOutbound(in, outSession)
	if len(body) > 0 {
		outReq.Body = io.NopCloser(bytes.NewReader(body))
		outReq.ContentLength = int64(len(body))
	}
	outReq.Header.Set("Accept", "text/event-stream")
	outReq.Header.Set("Cache-Control", "no-cache")
	if p.cfg.Dump {
		dumpOutbound(outReq, body, ns, incomingSession, outSession, p.cfg.Verbose)
	}
	outReq = outReq.WithContext(ctx)
	tr := p.egress.Transport(fam)
	resp, err := tr.RoundTrip(outReq)
	if err != nil {
		return nil, err
	}
	isStreamResp := strings.Contains(resp.Header.Get("Content-Type"), "event-stream")
	if p.cfg.Dump {
		dumpResponse(resp, fmt.Sprintf("本地 IPv%s 响应 %s", fam, in.URL.Path), false)
	}
	if !isStreamResp && resp.Body != nil && !p.cfg.Dump {
		nb, rerr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if rerr != nil {
			return nil, rerr
		}
		resp.Body = io.NopCloser(bytes.NewReader(nb))
		resp.ContentLength = int64(len(nb))
		resp.Header.Del("Transfer-Encoding")
		resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(nb)))
	}
	if p.cfg.Verbose {
		log.Printf("[opencode-proxy] %s %s%s -> IPv%s session:%q->%q status=%d stream=true ct=%q cl=%d", in.Method, p.cfg.BackendURL, in.URL.Path, fam, incomingSession, outSession, resp.StatusCode, resp.Header.Get("Content-Type"), resp.ContentLength)
	}
	return resp, nil
}

func (p *Proxy) doLocal(ctx context.Context, fam string, in *http.Request, body []byte) (*http.Response, error) {
	if isStreamRequest(body) {
		return p.doLocalStream(ctx, fam, in, body)
	}
	return p.doLocalBuffered(ctx, fam, in, body)
}

func (p *Proxy) HandleClusterForward(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
	}
	isStream := isStreamRequest(body)
	var ctx context.Context
	var cancel context.CancelFunc
	if isStream {
		ctx = context.Background()
	} else {
		ctx, cancel = context.WithTimeout(context.Background(), util.UpstreamTimeout)
		defer cancel()
	}
	if resp, err := p.tryLocal(ctx, r, body); err == nil {
		if p.cfg.Dump {
			dumpResponse(resp, fmt.Sprintf("集群代理响应 %s (本节点转发)", r.URL.Path), p.cfg.Verbose)
		}
		return resp, nil
	} else if p.cluster == nil || !p.cluster.Enabled() {
		return nil, err
	} else {
		return p.cluster.ForwardCluster(ctx, r, body)
	}
}

func (p *Proxy) tryLocal(ctx context.Context, r *http.Request, body []byte) (*http.Response, error) {
	order := p.egress.EgressOrder(r)
	// egressOrder 已处理 X-Egress/d4/d6/auto，这里仅去重不可用栈
	var lastErr error
	for _, fam := range order {
		if p.egress.IsUnavailable(fam) && p.egress.IsStackDown(fam) {
			continue
		}
		if resp, err := p.doLocal(ctx, fam, r, body); err == nil {
			return resp, nil
		} else {
			lastErr = err
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("无可用 egress")
}

// rewriteBodyModel 替换 JSON body 中的 "model" 字段; 非法 JSON 或无该字段时原样返回
func rewriteBodyModel(body []byte, model string) ([]byte, bool) {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return body, false
	}
	if _, ok := m["model"]; !ok {
		return body, false
	}
	m["model"] = model
	nb, err := json.Marshal(m)
	if err != nil {
		return body, false
	}
	return nb, true
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status = code
		r.wroteHeader = true
		r.ResponseWriter.WriteHeader(code)
	}
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(200)
	}
	return r.ResponseWriter.Write(b)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w, status: 200}
	defer func() {
		log.Printf("[proxy] %s %d %s", r.URL.Path, rec.status, time.Since(start).Truncate(time.Millisecond))
	}()
	w = rec
	if p.cluster != nil && (p.cluster.IsInternalPath(r.URL.Path) || r.URL.Path == "/_cluster/peers") {
		if p.cluster.HandleHTTP(w, r) {
			return
		}
	}
	if p.cfg.InboundAuth != "" {
		if !util.SecureCompare(r.Header.Get("Authorization"), "Bearer "+p.cfg.InboundAuth) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="opencode-zen-proxy"`)
			http.Error(w, "401 Unauthorized: missing or invalid Bearer token", http.StatusUnauthorized)
			return
		}
	}
	if p.cfg.Dump {
		dumpRequest(r, p.cfg.Verbose)
	}

	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	if p.cfg.RewriteModel != "" {
		if nb, ok := rewriteBodyModel(body, p.cfg.RewriteModel); ok {
			body = nb
		}
	}

	visitedIn, hopIn := cluster.ParseVisited(r)
	visitedMap := map[string]bool{}
	for _, v := range visitedIn {
		visitedMap[strings.TrimSpace(v)] = true
	}
	if p.cluster != nil && visitedMap[p.cluster.SelfID()] {
		http.Error(w, "loop detected", http.StatusLoopDetected)
		return
	}
	var visited []string
	var hop int
	if p.cluster != nil {
		visited, hop = cluster.BuildVisited(p.cluster.SelfID(), visitedIn, hopIn)
		visitedMap[p.cluster.SelfID()] = true
	} else {
		visited, hop = visitedIn, hopIn
	}
	_ = hop

	order := p.egress.EgressOrder(r)
	var lastResp *http.Response
	var lastErr error

	tryLocalFamilies := func(ctx context.Context) (*http.Response, bool) {
		if p.egress.EgressPrefer == "auto" && len(order) == 2 && !p.egress.IsStackDown(order[0]) && !p.egress.IsStackDown(order[1]) {
			type raceRes struct {
				resp *http.Response
				err  error
				fam  string
			}
			isStream := isStreamRequest(body)
			ctxRace, cancelRace := context.WithCancel(ctx)
			defer cancelRace()
			ch := make(chan raceRes, 2)
			for idx, fam := range order {
				fam := fam
				delay := time.Duration(idx) * 250 * time.Millisecond
				go func() {
					if delay > 0 {
						select {
						case <-time.After(delay):
						case <-ctxRace.Done():
							return
						}
					}
					ctx2 := ctxRace
					var cancel context.CancelFunc
					if !isStream {
						ctx2, cancel = context.WithTimeout(ctxRace, util.UpstreamTimeout)
						defer cancel()
					}
					resp, err := p.doLocal(ctx2, fam, r, body)
					select {
					case ch <- raceRes{resp: resp, err: err, fam: fam}:
					case <-ctxRace.Done():
						if resp != nil && resp.Body != nil {
							resp.Body.Close()
						}
					}
				}()
			}
			var fails int
			for fails < 2 {
				select {
				case res := <-ch:
					if res.err != nil {
						isTO := strings.Contains(res.err.Error(), "timeout") || strings.Contains(res.err.Error(), "deadline")
						if p.cluster != nil && p.cluster.Enabled() && isTO && !p.cluster.ShouldFailover(0, true) {
							lastErr = res.err
							return nil, false
						}
						p.egress.MarkUnavailable(res.fam, util.IsStackErrStatic(res.err))
						lastErr = res.err
						fails++
						continue
					}
					shouldFail := p.shouldRetryLocal(res.resp.StatusCode)
					if shouldFail {
						p.egress.MarkUnavailable(res.fam, false)
						lastResp = res.resp
						b, _ := io.ReadAll(res.resp.Body)
						res.resp.Body.Close()
						lastResp.Body = io.NopCloser(bytes.NewReader(b))
						if p.cluster != nil && p.cluster.Enabled() {
							log.Printf("[failover] 本地 IPv%s 返回 %d 可 failover, 尝试对端转发", res.fam, res.resp.StatusCode)
						} else {
							log.Printf("[retry] 本地 IPv%s 返回 %d 可重试, 尝试备用出口", res.fam, res.resp.StatusCode)
						}
						fails++
						if fails >= 2 {
							return nil, false
						}
						continue
					}
					p.egress.MarkAvailable(res.fam)
					cancelRace()
					go func() {
						for i := fails + 1; i < 2; i++ {
							select {
							case r2 := <-ch:
								if r2.resp != nil && r2.resp.Body != nil {
									r2.resp.Body.Close()
								}
							case <-time.After(2 * time.Second):
								return
							}
						}
					}()
					return res.resp, true
				case <-ctx.Done():
					lastErr = ctx.Err()
					return nil, false
				}
			}
			return nil, false
		}
		for _, fam := range order {
			if p.egress.IsUnavailable(fam) {
				continue
			}
			ctx2 := ctx
			var cancel context.CancelFunc
			if !isStreamRequest(body) {
				ctx2, cancel = context.WithTimeout(ctx, util.UpstreamTimeout)
			}
			resp, err := p.doLocal(ctx2, fam, r, body)
			if cancel != nil {
				cancel()
			}
			if err != nil {
				isTO := strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline")
				if p.cluster != nil && p.cluster.Enabled() && isTO && !p.cluster.ShouldFailover(0, true) {
					lastErr = err
					return nil, false
				}
				p.egress.MarkUnavailable(fam, util.IsStackErrStatic(err))
				lastErr = err
				continue
			}
			if p.shouldRetryLocal(resp.StatusCode) {
				p.egress.MarkUnavailable(fam, false)
				lastResp = resp
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				lastResp.Body = io.NopCloser(bytes.NewReader(b))
				hdr := lastResp.Header
				_ = hdr
				if p.cluster != nil && p.cluster.Enabled() {
					log.Printf("[failover] 本地 IPv%s 返回 %d 可 failover, 尝试对端转发", fam, resp.StatusCode)
				} else {
					log.Printf("[retry] 本地 IPv%s 返回 %d 可重试, 尝试备用出口", fam, resp.StatusCode)
				}
				continue
			}
			p.egress.MarkAvailable(fam)
			return resp, true
		}
		for _, fam := range order {
			if !p.egress.IsUnavailable(fam) {
				continue
			}
			if p.egress.IsStackDown(fam) {
				continue
			}
			ctx2 := ctx
			var cancel context.CancelFunc
			if !isStreamRequest(body) {
				ctx2, cancel = context.WithTimeout(ctx, util.UpstreamTimeout)
			}
			resp, err := p.doLocal(ctx2, fam, r, body)
			if cancel != nil {
				cancel()
			}
			if err != nil {
				if util.IsStackErrStatic(err) {
					p.egress.MarkStackDown(fam, true)
				}
				lastErr = err
				continue
			}
			if p.shouldRetryLocal(resp.StatusCode) {
				lastResp = resp
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				lastResp.Body = io.NopCloser(bytes.NewReader(b))
				if p.cluster != nil && p.cluster.Enabled() {
					log.Printf("[failover] 备用 IPv%s 亦 %d 可 failover", fam, resp.StatusCode)
				} else {
					log.Printf("[retry] 备用 IPv%s 亦 %d 可重试但已无更多出口", fam, resp.StatusCode)
				}
				continue
			}
			p.egress.MarkAvailable(fam)
			return resp, true
		}
		return nil, false
	}

	if resp, ok := tryLocalFamilies(r.Context()); ok {
		if isStreamRequest(body) && resp.StatusCode == 200 && strings.Contains(resp.Header.Get("Content-Type"), "event-stream") {
			for k, vv := range resp.Header {
				for _, v := range vv {
					if strings.EqualFold(k, "Content-Length") {
						continue
					}
					w.Header().Add(k, v)
				}
			}
			w.Header().Del("Content-Length")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("Connection", "keep-alive")
			w.WriteHeader(resp.StatusCode)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			notify := r.Context().Done()
			done := make(chan error, 1)
			go func() {
				_, err := io.Copy(w, resp.Body)
				done <- err
			}()
			select {
			case <-notify:
				resp.Body.Close()
				log.Printf("[sse] 客户端断开, 中止上游流")
				return
			case err := <-done:
				resp.Body.Close()
				if err != nil {
					log.Printf("[sse] 本地流结束 err=%v", err)
				}
				return
			}
		}
		defer resp.Body.Close()
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	if p.cluster != nil && p.cluster.Enabled() {
		peers := p.cluster.PickPeers(visitedMap)
		if len(peers) == 0 && lastResp != nil {
			log.Printf("[failover] 本地 %d 已 failover 但无可用 peer, 将原样返回 %d", lastResp.StatusCode, lastResp.StatusCode)
		}
		for _, peer := range peers {
			if hop+1 > cluster.MaxHop {
				break
			}
			log.Printf("[failover] 尝试对端 %s (hop %d)", peer.Addr, hop+1)
			peerCtx := r.Context()
			var cancel context.CancelFunc
			if !isStreamRequest(body) {
				peerCtx, cancel = context.WithTimeout(r.Context(), util.UpstreamTimeout)
			}
			cloneReq := &http.Request{Method: r.Method, URL: r.URL, Header: r.Header.Clone()}
			resp, err := p.cluster.ForwardToPeer(peerCtx, peer, cloneReq, body, visited, hop+1)
			if cancel != nil {
				cancel()
			}
			if err != nil {
				log.Printf("[failover] 对端 %s 失败: %v", peer.Addr, err)
				lastErr = err
				continue
			}
			if p.cluster.ShouldFailover(resp.StatusCode, false) {
				log.Printf("[failover] 对端 %s 亦 %d", peer.Addr, resp.StatusCode)
				lastResp = resp
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				lastResp.Body = io.NopCloser(bytes.NewReader(b))
				continue
			}
			log.Printf("[failover] 对端 %s 成功 %d", peer.Addr, resp.StatusCode)
			isStreamPeer := isStreamRequest(body) && resp.StatusCode == 200 && strings.Contains(resp.Header.Get("Content-Type"), "event-stream")
			if isStreamPeer {
				if p.cfg.Dump {
					dumpResponse(resp, fmt.Sprintf("集群对端响应 %s", r.URL.Path), false)
				}
				for k, vv := range resp.Header {
					if strings.EqualFold(k, "Content-Length") {
						continue
					}
					for _, v := range vv {
						w.Header().Add(k, v)
					}
				}
				w.Header().Del("Content-Length")
				w.Header().Set("Cache-Control", "no-cache")
				w.Header().Set("Connection", "keep-alive")
				w.WriteHeader(resp.StatusCode)
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				notify := r.Context().Done()
				done := make(chan error, 1)
				go func() {
					_, err := io.Copy(w, resp.Body)
					done <- err
				}()
				select {
				case <-notify:
					resp.Body.Close()
					log.Printf("[sse] 集群对端流 客户端断开, 中止")
					return
				case err := <-done:
					resp.Body.Close()
					if err != nil {
						log.Printf("[sse] 集群对端流结束 err=%v", err)
					}
					return
				}
			}
			nb, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if p.cfg.Dump {
				respD := &http.Response{StatusCode: resp.StatusCode, Header: resp.Header, Body: io.NopCloser(bytes.NewReader(nb))}
				dumpResponse(respD, fmt.Sprintf("集群对端响应 %s", r.URL.Path), p.cfg.Verbose)
			}
			preview := nb
			if len(preview) > 200 {
				preview = preview[:200]
			}
			log.Printf("[cluster] forward response bodyLen=%d hdr=%v preview=%q for %s", len(nb), resp.Header, string(preview), r.URL.Path)
			for k, vv := range resp.Header {
				for _, v := range vv {
					w.Header().Add(k, v)
				}
			}
			w.Header().Del("Transfer-Encoding")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(nb)))
			w.WriteHeader(resp.StatusCode)
			_, _ = w.Write(nb)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			return
		}
	}

	if lastResp != nil {
		defer lastResp.Body.Close()
		for k, vv := range lastResp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(lastResp.StatusCode)
		_, _ = io.Copy(w, lastResp.Body)
		return
	}
	if lastErr != nil {
		log.Printf("[error] 转发 %s %s 失败: %v", r.Method, r.URL.Path, lastErr)
		http.Error(w, "502 Bad Gateway: "+lastErr.Error(), http.StatusBadGateway)
		return
	}
	http.Error(w, "502 Bad Gateway", http.StatusBadGateway)
}
