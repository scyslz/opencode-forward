// opencode-proxy-net: 专属 opencode 反向代理 (仅 Go 标准库, 无第三方依赖)。
//
// 模仿 opencode CLI 的真实请求特征, 把完整 header 集自动注入到转发请求:
//
//	Authorization:      Bearer public                      (代码写死默认, 强制覆盖客户端; --auth 可改)
//	User-Agent:         opencode/1.15.0 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13
//	x-opencode-client:  cli                                 (固定, 不用每次换)
//	x-opencode-project: global                              (一般固定)
//	x-opencode-session: ses_xxx                             每次"会话"映射/缓存 (见会话缓存)
//	x-opencode-request: msg_xxx                             每次请求重新生成
//
// 会话缓存 (本地缓存, 按 出口IP家族 隔离, 切换 ip4/ip6 会话互不串号):
//   - 请求未带 x-opencode-session -> 生成随机 ses_xxx 插入到 header, 不放入 map
//   - 请求带了  x-opencode-session -> 映射 原始x-opencode-session -> 新ses_xxx,
//     命中缓存直接复用; 未命中则新建并把映射写入 map (可选 --cache-file 持久化)
//   - 缓存按 出口IP家族(4/6) 命名空间隔离: 同一原始session 在 IPv4 与 IPv6 出口
//     映射为两个不同的 ses_xxx, 保证 ip4/ip6 会话隔离 (核心)
//
// 用法:
//
//	opencode-proxy-net <监听端口> <backend> <4|6> [选项]
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	version          = "1.15.0"
	defaultUserAgent = "opencode/" + version + " ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13"
	defaultAuth      = "public"
	defaultClient    = "cli"
	defaultProject   = "global"
	base62           = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

func usage() {
	fmt.Fprintf(os.Stderr, `opencode-proxy-net - 专属 opencode 反向代理 (Go 标准库, 模仿 opencode CLI 真实特征)

用法:
  %s <监听端口> <backend> <4|6> [选项]

参数:
  监听端口   本地监听端口, 如 9000 或 0.0.0.0:9000
  backend    转发目标, 如 https://opencode.ai/zen 或 abc.com/121 (可带前缀路径)
  4|6        出口模式: 4=强制IPv4, 6=强制IPv6

自动注入的开源 CLI 特征头 (等价于 opencode zen 请求):
  Authorization:      Bearer public      强制覆盖客户端授权 (代码写死默认; --auth 可改)
  User-Agent:         opencode/1.15.0 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.13
  x-opencode-client:  cli              固定
  x-opencode-project: global           一般固定
  x-opencode-session: ses_xxx          会话映射/缓存 (见下)
  x-opencode-request: msg_xxx          每次请求重新生成

会话缓存 (本地缓存, 按 出口IP家族 隔离; 切换 ip4/ip6 会话互不串号 — 核心):
  请求未带 x-opencode-session -> 生成随机 ses_xxx 插入, 不放入 map
  请求带了 x-opencode-session -> 映射 原始->新ses_xxx, 命中缓存直接用, 否则新建并缓存
  缓存按 IPv4/IPv6 命名空间隔离: 同一原始session 在 ip4 与 ip6 出口映射为不同 ses_xxx
  --cache-file <path> 把该映射持久化为嵌套 JSON {4:{..}, 6:{..}}, 重启/切网络仍保持
  (ip4/ip6 两个实例可共享同一缓存文件, 各自读写自己的命名空间)

选项:
  --verbose            打印每条请求日志 (方法/路径/会话映射)
  --dump               打印完整转发请求特征: 全部 header + body (用于抓 opencode 真实特征)
  --auth <token>       Authorization: Bearer <token> (默认 public)
  --inbound-auth <token>  客户端访问本代理需带 Authorization: Bearer <token>
                        不匹配返回 401 (默认关闭; 只校验入站, 不影响转发给后端的 --auth)
  --header "K: v"      追加任意头, 可重复; 优先级最高, 覆盖默认/自动头
  --cache-file <path>  会话映射持久化文件(JSON)
  --xff                追加 X-Forwarded-For (真实客户端 IP)
  --gen-request        兼容旧参数(现在 request 每次必生成, 该开关已无意义)

  [头优先级]  --auth/--header > 自动生成(会话/request) > 默认特征头 > 客户端头

示例:
  %s 9000 https://opencode.ai/zen 4
  %s 9000 https://opencode.ai/zen 6 --auth sk-xxx --cache-file ./logs/sess.json --verbose
`, os.Args[0], os.Args[0], os.Args[0])
	os.Exit(1)
}

// joinPath 拼接后端基础路径 base 与请求路径 p，保证中间只有一个 '/';
// base 结尾带不带 '/' 都行。
func joinPath(base, p string) string {
	if base == "" {
		if p == "" {
			return "/"
		}
		return p
	}
	if p == "" {
		return base
	}
	q := strings.TrimPrefix(p, "/")
	if strings.HasSuffix(base, "/") {
		return base + q
	}
	return base + "/" + q
}

// parseBackend 解析 backend 参数，返回 scheme / host / 基础路径。
func parseBackend(arg string) (string, string, string, error) {
	u := arg
	if !strings.Contains(arg, "://") {
		u = "http://" + arg // 默认 http
	}
	parsed, err := url.Parse(u)
	if err != nil {
		return "", "", "", fmt.Errorf("解析 backend 失败: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", "", "", fmt.Errorf("不支持的 scheme: %s (仅支持 http/https)", parsed.Scheme)
	}
	if parsed.Host == "" {
		return "", "", "", fmt.Errorf("backend 缺少 host: %s", arg)
	}
	basePath := parsed.Path
	if basePath == "" {
		basePath = "/"
	}
	if !strings.HasPrefix(basePath, "/") {
		basePath = "/" + basePath
	}
	return parsed.Scheme, parsed.Host, basePath, nil
}

// makeDialContext 生成按模式(4/6)强制选择 IP 家族的拨号函数。
func makeDialContext(mode string) func(ctx context.Context, network, addr string) (net.Conn, error) {
	baseDialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		var ips []net.IP
		if ip := net.ParseIP(host); ip != nil {
			ips = []net.IP{ip}
		} else {
			addrs, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
			if err != nil {
				return nil, fmt.Errorf("解析 %s 失败: %w", host, err)
			}
			ips = addrs
		}
		var lastErr error
		for _, ip := range ips {
			if mode == "4" && ip.To4() == nil {
				continue
			}
			if mode == "6" && ip.To4() != nil {
				continue
			}
			conn, err := baseDialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("没有可用 %s 地址: %s", map[string]string{"4": "IPv4", "6": "IPv6"}[mode], host)
		}
		return nil, lastErr
	}
}

// randomHex 生成 n 字节(2n 个 hex 字符)的加密随机串。
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

// secureCompare 常量时间比较两个字符串 (用于令牌校验, 防时序侧信道)。
func secureCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// opencodeID 仿 opencode 的 Identifier.create:
//
//	prefix + "_" + <12 hex 时间字节> + <14 base62> = prefix_ + 26 个字符
//	session -> "ses_xxx", message -> "msg_xxx"  (total 29 字符)
func opencodeID(prefix string) string {
	var sb strings.Builder
	sb.WriteString(prefix)
	sb.WriteByte('_')
	b6 := make([]byte, 6)
	_, _ = rand.Read(b6)
	sb.WriteString(hex.EncodeToString(b6)) // 12 hex
	b14 := make([]byte, 14)
	_, _ = rand.Read(b14)
	for _, b := range b14 {
		sb.WriteByte(base62[int(b)%len(base62)])
	}
	return sb.String()
}

// sessionCache 本地会话映射缓存, 按 出口IP家族(4/6) 隔离:
//
//	family("4"|"6") -> (原始 x-opencode-session -> 生成的 ses_xxx)
//
// 核心: 切换 ip4/ip6 会话必须隔离。同一原始 session 在 IPv4 出口 与 IPv6 出口
// 映射到两个不同的 ses_xxx, 互不串号。共享同一 --cache-file 的 ip4/ip6 两个实例
// 各自读写自己的 family 命名空间, 且落盘前会重读磁盘以保留兄弟实例的条目。
type sessionCache struct {
	mu     sync.Mutex
	family string                       // "4" 或 "6"
	path   string                       // 持久化文件 (可选)
	m      map[string]map[string]string // family -> (incoming -> ses_xxx)
}

func newSessionCache(family, path string) *sessionCache {
	c := &sessionCache{family: family, path: path, m: map[string]map[string]string{}}
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

// resolve 处理单个请求的 x-opencode-session (在当前 family 内隔离)。
//   - 未带 session: 生成随机 ses_xxx 并插入, 不放入 map
//   - 带了 session: 映射 原始->生成的 ses_xxx, 命中缓存直接用, 否则新建并放入 map
//
// 返回要写入转发请求的 x-opencode-session 值。
func (c *sessionCache) resolve(incoming string) string {
	if incoming == "" {
		return opencodeID("ses") // 不放入 map
	}
	if c.path != "" {
		return c.resolvePersist(incoming)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	fam := c.m[c.family]
	if fam == nil {
		fam = map[string]string{}
		c.m[c.family] = fam
	}
	if v, ok := fam[incoming]; ok {
		return v
	}
	v := opencodeID("ses")
	fam[incoming] = v
	return v
}

// resolvePersist 同 resolve, 但新建映射时同步落盘 (原子写)。
// 写盘前先重读磁盘, 合并兄弟 family 实例新增的条目, 避免相互覆盖。
func (c *sessionCache) resolvePersist(incoming string) string {
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
	fam := c.m[c.family]
	if fam == nil {
		fam = map[string]string{}
		c.m[c.family] = fam
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

// sessLen 返回当前 family 命名的会话条数。
func sessLen(c *sessionCache) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m[c.family])
}

// authSummary 启动日志用: 展示 Authorization 处理策略。
func authSummary(authToken string) string {
	if authToken != "" {
		return "Bearer " + authToken
	}
	return "保留客户端token(无则默认 Bearer public)"
}

// dumpRequest 打印完整请求特征 (方法/URL/全部 header/body)。
// 读取后把 body 恢复, 不影响后续转发。
func dumpRequest(r *http.Request) {
	var b strings.Builder
	b.WriteString("\n----- opencode 请求特征 -----\n")
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
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		if len(body) > 0 {
			b.WriteString("\nBODY:\n" + string(body) + "\n")
		}
	}
	b.WriteString("----- end -----")
	log.Print(b.String())
}

func main() {
	if len(os.Args) < 4 {
		usage()
	}
	listenArg := os.Args[1]
	backendArg := os.Args[2]
	mode := os.Args[3]

	verbose := false
	dump := false
	xff := false
	genRequest := false      // 兼容旧参数, 现在 request 每次必生成, 无实际作用
	authToken := defaultAuth // 代码写死默认 Bearer public; --auth 可改
	inboundAuth := ""        // 入站客户端校验 token, 空=关闭; --inbound-auth 可开
	cacheFile := ""
	var extraHeaders []string         // --header "K: v" 可重复
	var defaults = map[string]string{ // 默认特征头 (--header 可覆盖)
		"User-Agent":         defaultUserAgent,
		"X-Opencode-Client":  defaultClient,
		"X-Opencode-Project": defaultProject,
	}
	_ = genRequest // 保留兼容

	for i := 4; i < len(os.Args); i++ {
		arg := os.Args[i]
		switch {
		case arg == "--verbose":
			verbose = true
		case arg == "--dump":
			dump = true
		case arg == "--xff":
			xff = true
		case arg == "--gen-request":
			genRequest = true
		case arg == "--auth":
			if i+1 < len(os.Args) {
				i++
				authToken = os.Args[i]
			} else {
				fmt.Fprintln(os.Stderr, "错误: --auth 需要 token 参数")
				os.Exit(1)
			}
		case arg == "--inbound-auth":
			if i+1 < len(os.Args) {
				i++
				inboundAuth = os.Args[i]
			} else {
				fmt.Fprintln(os.Stderr, "错误: --inbound-auth 需要 token 参数")
				os.Exit(1)
			}
		case arg == "--cache-file":
			if i+1 < len(os.Args) {
				i++
				cacheFile = os.Args[i]
			} else {
				fmt.Fprintln(os.Stderr, "错误: --cache-file 需要路径参数")
				os.Exit(1)
			}
		case arg == "--header":
			if i+1 < len(os.Args) {
				i++
				extraHeaders = append(extraHeaders, os.Args[i])
			} else {
				fmt.Fprintln(os.Stderr, "错误: --header 需要 \"Name: value\" 参数")
				os.Exit(1)
			}
		default:
			fmt.Fprintf(os.Stderr, "未知参数: %s\n", arg)
			usage()
		}
	}
	if mode != "4" && mode != "6" {
		fmt.Fprintf(os.Stderr, "错误: 模式必须是 4 或 6，收到 %q\n", mode)
		usage()
	}

	scheme, host, basePath, err := parseBackend(backendArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		os.Exit(1)
	}
	backendURL := &url.URL{Scheme: scheme, Host: host}

	listenAddr := listenArg
	if !strings.Contains(listenAddr, ":") {
		listenAddr = ":" + listenAddr
	}

	sess := newSessionCache(mode, cacheFile)

	// Rewrite: 改写转发请求的 scheme/host/path, 并注入 opencode 特征头。
	rewrite := func(pr *httputil.ProxyRequest) {
		out := pr.Out
		out.URL.Scheme = scheme
		out.URL.Host = host // URL.Host 用于实际连接后端(拨号/SNI)
		out.URL.Path = joinPath(basePath, out.URL.Path)
		out.URL.RawPath = ""
		out.Host = host // 默认改写 Host: 后端以为请求直连它(命令行可覆盖见下)

		// 1) 默认特征头 (最低优先级): 强制替换客户端同名字头
		for k, v := range defaults {
			out.Header.Set(k, v)
		}

		// 2) 会话: 映射/生成 ses_xxx; request: 每次重新生成 msg_xxx
		incomingSession := pr.In.Header.Get("X-Opencode-Session")
		outSession := sess.resolve(incomingSession)
		out.Header.Set("X-Opencode-Session", outSession)
		out.Header.Set("X-Opencode-Request", opencodeID("msg"))

		// 3) --xff: 自动头 (默认会替换客户端值)
		if xff {
			if clientIP, _, err := net.SplitHostPort(pr.In.RemoteAddr); err == nil {
				out.Header.Set("X-Forwarded-For", clientIP)
			}
		}

		// 4) Authorization: 强制注入 Bearer public(代码写死默认, 强制覆盖客户端授权)。
		//    --auth/--header (最高优先级) 仍可覆盖; --header 又优先于 --auth。
		out.Header.Set("Authorization", "Bearer "+authToken)
		for _, h := range extraHeaders {
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

		// 5) 日志
		if verbose {
			log.Printf("[opencode-proxy] %s %s%s -> IPv%s session:%q->%q",
				pr.In.Method, backendURL, pr.In.URL.Path, mode, incomingSession, outSession)
		}
	}

	transport := &http.Transport{
		DialContext:         makeDialContext(mode),
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	proxy := &httputil.ReverseProxy{
		Rewrite:       rewrite,
		Transport:     transport,
		FlushInterval: -1, // 立即转发, 适合长连接/websocket/大文件
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("[error] 转发 %s %s 失败: %v", r.Method, r.URL.Path, err)
			http.Error(w, "502 Bad Gateway: "+err.Error(), http.StatusBadGateway)
		},
	}

	// 包装 handler: --dump 在转发前打印原始客户端请求 (完整 header + body)
	var handler http.Handler = proxy
	if verbose || dump {
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if dump {
				dumpRequest(r)
			}
			proxy.ServeHTTP(w, r)
		})
	}

	// 入站校验: --inbound-auth 开启后, 客户端必须先带 Authorization: Bearer <token>,
	// 否则直接 401, 不进入转发放行。默认关闭 (inboundAuth=="")。
	if inboundAuth != "" {
		expected := "Bearer " + inboundAuth
		authorized := handler
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !secureCompare(r.Header.Get("Authorization"), expected) {
				w.Header().Set("WWW-Authenticate", `Bearer realm="opencode-proxy-net"`)
				http.Error(w, "401 Unauthorized: missing or invalid Bearer token", http.StatusUnauthorized)
				return
			}
			authorized.ServeHTTP(w, r)
		})
	}

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误: 监听 %s 失败: %v\n", listenAddr, err)
		os.Exit(1)
	}

	srv := &http.Server{
		Handler:        handler,
		ReadTimeout:    60 * time.Second,
		WriteTimeout:   0, // 流式传输不设写超时
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Printf("opencode-proxy-net 启动 (version %s):", version)
	log.Printf("  监听: %s (双栈 IPv4+IPv6)", ln.Addr())
	log.Printf("  后端: %s//%s  基础路径: %s", scheme, host, basePath)
	log.Printf("  模式: IPv%s (出口强制 IPv%s)", mode, mode)
	log.Printf("  特征头: User-Agent=%s client=%s project=%s auth=%s",
		defaultUserAgent, defaultClient, defaultProject, authSummary(authToken))
	if inboundAuth != "" {
		log.Printf("  入站校验: 开启 (客户端需 Authorization: Bearer %s)", inboundAuth)
	} else {
		log.Printf("  入站校验: 关闭 (默认)")
	}
	if cacheFile != "" {
		log.Printf("  会话缓存: 持久化到 %s (IPv%s 命名空间隔离, 共 %d 条)", cacheFile, mode, sessLen(sess))
	} else {
		log.Printf("  会话缓存: 仅内存 (IPv%s 命名空间隔离, 不持久化)", mode)
	}
	log.Printf("  示例: GET /a -> %s%s/a", backendURL, basePath)

	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Fatalf("服务异常退出: %v", err)
	}
}
