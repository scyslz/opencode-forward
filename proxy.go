package main

import (
	"bytes"
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

type sessionCache struct {
	mu   sync.Mutex
	path string
	m    map[string]map[string]string
}

func newSessionCache(path string) *sessionCache {
	c := &sessionCache{path: path, m: map[string]map[string]string{}}
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

func (c *sessionCache) resolve(nsKey, incoming string) string {
	if incoming == "" {
		return opencodeID("ses")
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
	v := opencodeID("ses")
	fam[incoming] = v
	return v
}

func (c *sessionCache) resolvePersist(nsKey, incoming string) string {
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
	v := opencodeID("ses")
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

func rewriteStreamFalse(body []byte) []byte {
	nb := bytes.ReplaceAll(body, []byte(`"stream":true`), []byte(`"stream":false`))
	nb = bytes.ReplaceAll(nb, []byte(`"stream": true`), []byte(`"stream":false`))
	nb = bytes.ReplaceAll(nb, []byte(`"stream":1`), []byte(`"stream":false`))
	return nb
}

func isEmptySSE(data []byte) bool {
	if len(data) == 0 {
		return true
	}
	s := string(data)
	if strings.Contains(s, `"delta"`) && strings.Contains(s, `"content"`) {
		return false
	}
	if strings.Contains(s, `"choices":[]`) || strings.Contains(s, `"choices": []`) {
		if !strings.Contains(s, `"text"`) && !strings.Contains(s, `"content":"`) {
			return true
		}
	}
	trim := strings.TrimSpace(s)
	if trim == "" {
		return true
	}
	return false
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
	uri := r.URL.String()
	if r.URL.RawQuery != "" {
		uri = r.URL.Path + "?" + r.URL.RawQuery
	} else {
		uri = r.URL.Path
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

type proxyConfig struct {
	scheme       string
	host         string
	basePath     string
	backendURL   *url.URL
	authToken    string
	inboundAuth  string
	fwdInbound   bool
	xff          bool
	verbose      bool
	dump         bool
	extraHeaders []string
	defaults     map[string]string
}

type Proxy struct {
	cfg     proxyConfig
	egress  *egressManager
	sess    *sessionCache
	cluster ClusterForwarder
}

type ClusterForwarder interface {
	Enabled() bool
	ShouldFailover(status int, isTimeout bool) bool
	PickPeers(visited map[string]bool) []*clusterPeer
	ForwardToPeer(ctx context.Context, peer *clusterPeer, orig *http.Request, body []byte, visited []string, hop int) (*http.Response, error)
	HandleHTTP(w http.ResponseWriter, r *http.Request) bool
	IsInternalPath(p string) bool
	SelfID() string
}

func newProxy(cfg proxyConfig, egress *egressManager, sess *sessionCache) *Proxy {
	return &Proxy{cfg: cfg, egress: egress, sess: sess}
}

func (p *Proxy) SetCluster(c ClusterForwarder) {
	p.cluster = c
	if cn, ok := c.(*clusterNode); ok && cn != nil {
		cn.SetForwarder(p.DoClusterForward)
	}
}

func (p *Proxy) buildOutbound(in *http.Request, outSession string) *http.Request {
	u := &url.URL{Scheme: p.cfg.scheme, Host: p.cfg.host, Path: joinPath(p.cfg.basePath, in.URL.Path), RawQuery: in.URL.RawQuery}
	out := &http.Request{
		Method: in.Method,
		URL:    u,
		Header: http.Header{},
		Host:   p.cfg.host,
	}
	for k, vv := range in.Header {
		if strings.EqualFold(k, "X-Egress") || strings.EqualFold(k, clusterHdrVisited) || strings.EqualFold(k, clusterHdrHop) || strings.EqualFold(k, clusterHdrToken) {
			continue
		}
		for _, v := range vv {
			out.Header.Add(k, v)
		}
	}
	for k, v := range p.cfg.defaults {
		out.Header.Set(k, v)
	}
	out.Header.Set("X-Opencode-Session", outSession)
	out.Header.Set("X-Opencode-Request", opencodeID("msg"))
	if p.cfg.xff {
		if clientIP, _, err := net.SplitHostPort(in.RemoteAddr); err == nil {
			out.Header.Set("X-Forwarded-For", clientIP)
		}
	}
	if p.cfg.authToken != "" {
		tok := p.cfg.authToken
		if !strings.HasPrefix(strings.ToLower(tok), "bearer ") {
			tok = "Bearer " + tok
		}
		out.Header.Set("Authorization", tok)
	} else if p.cfg.fwdInbound && p.cfg.inboundAuth != "" {
		tok := p.cfg.inboundAuth
		if !strings.HasPrefix(strings.ToLower(tok), "bearer ") {
			tok = "Bearer " + tok
		}
		out.Header.Set("Authorization", tok)
	}
	for _, h := range p.cfg.extraHeaders {
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

func (p *Proxy) doLocal(ctx context.Context, fam string, in *http.Request, body []byte) (*http.Response, error) {
	ns := p.egress.nsKey(fam)
	incomingSession := in.Header.Get("X-Opencode-Session")
	outSession := p.sess.resolve(ns, incomingSession)
	outReq := p.buildOutbound(in, outSession)
	if len(body) > 0 {
		outReq.Body = io.NopCloser(bytes.NewReader(body))
		outReq.ContentLength = int64(len(body))
	}
	isStream := bytes.Contains(body, []byte(`"stream":true`)) || bytes.Contains(body, []byte(`"stream": true`))
	if isStream {
		outReq.Header.Set("Accept", "text/event-stream")
		outReq.Header.Set("Cache-Control", "no-cache")
	}
	if p.cfg.dump {
		dumpOutbound(outReq, body, ns, incomingSession, outSession, p.cfg.verbose)
	}
	outReq = outReq.WithContext(ctx)
	tr := p.egress.transport(fam)
	resp, err := tr.RoundTrip(outReq)
	if err != nil {
		return nil, err
	}
	if p.cfg.verbose {
		log.Printf("[opencode-proxy] %s %s%s -> IPv%s session:%q->%q status=%d stream=%v ct=%q cl=%d", in.Method, p.cfg.backendURL, in.URL.Path, fam, incomingSession, outSession, resp.StatusCode, isStream, resp.Header.Get("Content-Type"), resp.ContentLength)
	}
	return resp, nil
}

func (p *Proxy) DoClusterForward(r *http.Request) (*http.Response, error) {
	var body []byte
	if r.Body != nil {
		body, _ = io.ReadAll(r.Body)
	}
	egress := r.Header.Get(clusterHdrEgress)
	if egress != "4" && egress != "6" {
		egress = p.egress.egressPrefer
		if egress == "auto" {
			egress = "6"
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := p.doLocal(ctx, egress, r, body)
	if err != nil {
		other := "4"
		if egress == "4" {
			other = "6"
		}
		if !p.egress.isUnavailable(other) {
			if r2, err2 := p.doLocal(ctx, other, r, body); err2 == nil {
				return r2, nil
			}
		}
		return nil, err
	}
	if p.cluster != nil && p.cluster.ShouldFailover(resp.StatusCode, false) {
		other := "4"
		if egress == "4" {
			other = "6"
		}
		if !p.egress.isUnavailable(other) {
			resp.Body.Close()
			if r2, err2 := p.doLocal(ctx, other, r, body); err2 == nil && !p.cluster.ShouldFailover(r2.StatusCode, false) {
				return r2, nil
			} else if err2 == nil {
				resp = r2
			}
		}
	}
	return resp, nil
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if p.cluster != nil && (p.cluster.IsInternalPath(r.URL.Path) || r.URL.Path == "/_cluster/peers") {
		if p.cluster.HandleHTTP(w, r) {
			return
		}
	}
	if p.cfg.inboundAuth != "" {
		if !secureCompare(r.Header.Get("Authorization"), "Bearer "+p.cfg.inboundAuth) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="opencode-zen-proxy"`)
			http.Error(w, "401 Unauthorized: missing or invalid Bearer token", http.StatusUnauthorized)
			return
		}
	}
	if p.cfg.dump {
		dumpRequest(r, p.cfg.verbose)
	}

	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	visitedIn, hopIn := parseVisited(r)
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
		visited, hop = buildVisited(p.cluster.SelfID(), visitedIn, hopIn)
		visitedMap[p.cluster.SelfID()] = true
	} else {
		visited, hop = visitedIn, hopIn
	}
	_ = hop

	order := p.egress.egressOrder(r)
	var lastResp *http.Response
	var lastErr error

	tryLocalFamilies := func(ctx context.Context) (*http.Response, bool) {
		for _, fam := range order {
			if p.egress.isUnavailable(fam) {
				continue
			}
			ctx2 := ctx
			var cancel context.CancelFunc
			if !isStreamRequest(body) {
				ctx2, cancel = context.WithTimeout(ctx, 30*time.Second)
			}
			resp, err := p.doLocal(ctx2, fam, r, body)
			if cancel != nil {
				cancel()
			}
			if err != nil {
				isTO := strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline")
				if isTO && p.cluster != nil && !p.cluster.ShouldFailover(0, true) {
					lastErr = err
					return nil, false
				}
				p.egress.markUnavailable(fam, isStackErrStatic(err))
				lastErr = err
				continue
			}
			shouldFail := p.cluster != nil && p.cluster.ShouldFailover(resp.StatusCode, false)
			if shouldFail {
				p.egress.markUnavailable(fam, false)
				lastResp = resp
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				lastResp.Body = io.NopCloser(bytes.NewReader(b))
				hdr := lastResp.Header
				_ = hdr
				log.Printf("[failover] 本地 IPv%s 返回 %d 可 failover, 尝试对端转发", fam, resp.StatusCode)
				continue
			}
			p.egress.markAvailable(fam)
			return resp, true
		}
		for _, fam := range order {
			if !p.egress.isUnavailable(fam) {
				continue
			}
			if p.egress.isStackDown(fam) {
				continue
			}
			ctx2 := ctx
			var cancel context.CancelFunc
			if !isStreamRequest(body) {
				ctx2, cancel = context.WithTimeout(ctx, 30*time.Second)
			}
			resp, err := p.doLocal(ctx2, fam, r, body)
			if cancel != nil {
				cancel()
			}
			if err != nil {
				if isStackErrStatic(err) {
					p.egress.markStackDown(fam, true)
				}
				lastErr = err
				continue
			}
			shouldFail := p.cluster != nil && p.cluster.ShouldFailover(resp.StatusCode, false)
			if shouldFail {
				lastResp = resp
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				lastResp.Body = io.NopCloser(bytes.NewReader(b))
				log.Printf("[failover] 备用 IPv%s 亦 %d 可 failover", fam, resp.StatusCode)
				continue
			}
			p.egress.markAvailable(fam)
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
			if hop+1 > maxHop {
				break
			}
			log.Printf("[failover] 尝试对端 %s (hop %d)", peer.Addr, hop+1)
			peerCtx := r.Context()
			var cancel context.CancelFunc
			if !isStreamRequest(body) {
				peerCtx, cancel = context.WithTimeout(r.Context(), 30*time.Second)
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

func (p *Proxy) responseLooksEmpty(resp *http.Response) bool {
	ct := resp.Header.Get("Content-Type")
	if ct != "" && !strings.Contains(ct, "text/event-stream") && !strings.Contains(ct, "event-stream") {
		return false
	}
	return false
}



func (p *Proxy) retryAsNonStream(body []byte, order []string) *http.Response {
	nb := rewriteStreamFalse(body)
	var fixed *http.Response
	for _, fam := range order {
		if p.egress.isUnavailable(fam) {
			continue
		}
		ctx2, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		tmpReq := &http.Request{Method: "POST", URL: mustParse("/chat/completions"), Header: http.Header{"Content-Type": []string{"application/json"}}}
		resp2, err2 := p.doLocal(ctx2, fam, tmpReq, nb)
		cancel()
		if err2 != nil {
			continue
		}
		if resp2.StatusCode == 200 {
			fixed = resp2
			break
		}
		resp2.Body.Close()
	}
	if fixed == nil {
		return nil
	}
	fb, _ := io.ReadAll(fixed.Body)
	fixed.Body.Close()
	var m map[string]any
	if json.Unmarshal(fb, &m) == nil {
		if ch, ok := m["choices"].([]any); ok && len(ch) > 0 {
			if c0, ok := ch[0].(map[string]any); ok {
				if msg, ok := c0["message"].(map[string]any); ok {
					content, _ := msg["content"].(string)
					if content != "" {
						id, _ := m["id"].(string)
						if id == "" {
							id = opencodeID("resp")
						}
						sse := fmt.Sprintf("data: {\"id\":\"%s\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":%s}}]}\n\ndata: [DONE]\n", id, mustJSONString(content))
						fixed.Body = io.NopCloser(strings.NewReader(sse))
						fixed.Header.Set("Content-Type", "text/event-stream")
						fixed.Header.Set("Cache-Control", "no-cache")
						fixed.Header.Del("Content-Length")
						fixed.Header.Set("Transfer-Encoding", "chunked")
						return fixed
					}
				}
			}
		}
	}
	fixed.Body = io.NopCloser(bytes.NewReader(fb))
	return fixed
}

func mustParse(p string) *url.URL    { u, _ := url.Parse(p); return u }
func mustJSONString(s string) string { b, _ := json.Marshal(s); return string(b) }
