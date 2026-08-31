package main

import (
	"fmt"
	"opencode-zen-proxy/internal/cluster"
	"opencode-zen-proxy/internal/egress"
	"opencode-zen-proxy/internal/proxy"
	"opencode-zen-proxy/internal/util"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	version          = "1.18.29"
	defaultUserAgent = "opencode/" + version + " ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"
	defaultClient    = "cli"
	defaultProject   = "global"
)

func usage() {
	fmt.Fprintf(os.Stderr, `opencode-zen-proxy - 专属 opencode 流量转发 (Go 标准库, 模仿 opencode CLI 真实特征)

用法:
  %s <监听端口> <backend> [选项]
  兼容: %s <监听端口> <backend> <4|6> [选项]  (兼容旧版, 4|6映射为--egress-prefer)

参数:
  监听端口   本地监听端口, 如 9000 或 0.0.0.0:9000 (单口双栈)
  backend    转发目标, 如 https://opencode.ai/zen 或 abc.com/121 (可带前缀路径)

自动注入的开源 CLI 特征头:
  Authorization:      Bearer <token>       --outbound-auth 注入并覆盖客户端授权
  User-Agent:         opencode/1.18.29 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14
  x-opencode-client:  cli              固定
  x-opencode-project: global           一般固定
  x-opencode-session: ses_xxx          会话映射/缓存
  x-opencode-request: msg_xxx          每次请求重新生成

会话缓存 (按 当前具体出口IP 隔离; 出口IP变化自动换新会话):
  每次请求前读取最新出口IP, 以 "6|1:2:3::1" 作为命名空间 key; IP 变化->自动换新会话
  探测失败不退出, IP 未知时退化为 "4"/"6", 后台自动重试, 恢复即切回
  后台每 --ip-interval (默认 5m) 探测一次出口IP
  --cache-file <path> 持久化为嵌套 JSON {nsKey:{..}}

选项:
  --verbose            打印每条请求日志 (等价 --log-level debug)
  --log-level <lv>     日志级别 debug/info/warn/error (默认 info; 集群重试/ping仅debug)
  --dump               打印完整转发请求特征: 全部 header + body
  --model <名称>        替换请求体 JSON 的 "model" 字段 (空=不替换, 默认)
  --outbound-auth <token>  转发给后端 Authorization: Bearer <token>
  --inbound-auth <token>   客户端访问本服务需带 Authorization: Bearer <token>
  -F, --forward-inbound-auth  转发使用 inbound-auth 的 token 作为后端 Authorization
  --header "K: v"      追加任意头, 可重复; 优先级最高, 覆盖默认/自动头
  --cache-file <path>  会话映射持久化文件(JSON)
  --tunnel-file <path> 集群隧道连接信息持久化文件(JSON), 记录建立/关闭/帧数
  --xff                追加 X-Forwarded-For
  --ip-interval <dur>  出口IP探测周期, 如 5m (默认 5m)
  --ip-url <url>       出口IP探测服务 (默认 IPv4: https://api.ipify.org, IPv6: https://api6.ipify.org)
  --proxy <url>        经代理出口, http/https/socks5/socks5h, 如 socks5://127.0.0.1:1080 (优先代理, 不区分4/6)
  --proxy-probe-interval <dur>  代理出口IP探测周期, 默认 30s
  --egress-prefer <4|6|auto>  本机出口优先级, 默认 6 (6→4; auto为并发HappyEyeballs) (别名 --prefer)
  --cluster-id <id>    集群节点ID (默认随机)
  --cluster-token <t>  集群鉴权token (常量时间比较)
  --cluster-listen <addr>  集群私有协议监听, 如 :9443
  --cluster-join <addr>    私网主动外拨加入公网节点, 如 公网:9443 (反向隧道)
  --peer <addr>        集群对等节点, 可重复, 如 peer2:9443
  --failover-on <list> 触发集群转发的状态码/超时, 如 429,502,503,504,timeout (默认同)

  [头优先级]  --outbound-auth/-F/--header > 自动生成(会话/request) > 默认特征头 > 客户端头

示例:
  %s 9000 https://opencode.ai/zen
  %s 9000 https://opencode.ai/zen --outbound-auth sk-xxx --cache-file ./logs/sess.json --verbose
  %s 9000 https://opencode.ai/zen --cluster-id node1 --cluster-token s3 --cluster-listen :9443 --peer node2:9443
`, os.Args[0], os.Args[0], os.Args[0], os.Args[0], os.Args[0])
	os.Exit(1)
}

func authSummary(tok string) string { if tok != "" { return "Bearer "+tok }; return "透传客户端Authorization" }

func main() {
	if len(os.Args) < 3 {
		usage()
	}

	clusterCfg, rest := cluster.ParseClusterArgs(os.Args[1:])
	if len(rest) < 2 {
		usage()
	}
	listenArg := rest[0]
	backendArg := rest[1]
	extraArgs := rest[2:]

	egressPrefer := "6"
	if len(extraArgs) > 0 && (extraArgs[0] == "4" || extraArgs[0] == "6") {
		egressPrefer = extraArgs[0]
		extraArgs = extraArgs[1:]
	}

	verbose := false
	dump := false
	xff := false
	logLevelArg := ""
	authToken := ""
	inboundAuth := ""
	fwdInbound := false
	cacheFile := ""
	rewriteModel := ""
	probeInterval := 5 * time.Minute
	proxyProbeInterval := 30 * time.Second
	probeURL4 := ""
	probeURL6 := ""
	dnsServer := ""
	proxyURL := ""
	var extraHeaders []string
	var defaults = map[string]string{
		"User-Agent":         defaultUserAgent,
		"X-Opencode-Client":  defaultClient,
		"X-Opencode-Project": defaultProject,
	}

	for i := 0; i < len(extraArgs); i++ {
		arg := extraArgs[i]
		switch {
		case arg == "--verbose":
			verbose = true
		case arg == "--dump":
			dump = true
		case arg == "--xff":
			xff = true
		case arg == "--log-level":
			if i+1 < len(extraArgs) {
				i++
				logLevelArg = extraArgs[i]
				util.SetLogLevel(logLevelArg)
			}
		case arg == "--egress-prefer" || arg == "--prefer":
			if i+1 < len(extraArgs) {
				i++
				egressPrefer = extraArgs[i]
				switch egressPrefer {
				case "4", "6", "auto", "d4", "d6":
				default:
					fmt.Fprintln(os.Stderr, "错误: --prefer/--egress-prefer 必须是 4/6/auto/d4/d6")
					os.Exit(1)
				}
			}
		case arg == "--ip-interval":
			if i+1 < len(extraArgs) {
				i++
				dur, err := time.ParseDuration(extraArgs[i])
				if err != nil || dur <= 0 {
					fmt.Fprintf(os.Stderr, "错误: --ip-interval 需要合法时长, 如 5m")
					os.Exit(1)
				}
				probeInterval = dur
			}
		case arg == "--dns-server":
			if i+1 < len(extraArgs) {
				i++
				dnsServer = extraArgs[i]
			}
		case arg == "--proxy":
			if i+1 < len(extraArgs) {
				i++
				proxyURL = extraArgs[i]
			}
		case arg == "--proxy-probe-interval":
			if i+1 < len(extraArgs) {
				i++
				dur, err := time.ParseDuration(extraArgs[i])
				if err != nil || dur <= 0 {
					fmt.Fprintf(os.Stderr, "错误: --proxy-probe-interval 需要合法时长, 如 30s")
					os.Exit(1)
				}
				proxyProbeInterval = dur
			}
		case arg == "--ip-url":
			if i+1 < len(extraArgs) {
				i++
				u := extraArgs[i]
				probeURL4 = u
				probeURL6 = u
			}
		case arg == "--outbound-auth" || arg == "--auth":
			if i+1 < len(extraArgs) {
				i++
				authToken = extraArgs[i]
			}
		case arg == "--forward-inbound-auth" || arg == "-F":
			fwdInbound = true
		case arg == "--inbound-auth":
			if i+1 < len(extraArgs) {
				i++
				inboundAuth = extraArgs[i]
			}
		case arg == "--cache-file":
			if i+1 < len(extraArgs) {
				i++
				cacheFile = extraArgs[i]
			}
		case arg == "--model":
			if i+1 < len(extraArgs) {
				i++
				rewriteModel = extraArgs[i]
			}
		case arg == "--header":
			if i+1 < len(extraArgs) {
				i++
				extraHeaders = append(extraHeaders, extraArgs[i])
			}
		default:
			fmt.Fprintf(os.Stderr, "未知参数: %s\n", arg)
			usage()
		}
	}

	scheme, host, basePath, err := util.ParseBackend(backendArg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		os.Exit(1)
	}
	backendURL := &url.URL{Scheme: scheme, Host: host}
	listenAddr := listenArg
	if !strings.Contains(listenAddr, ":") {
		listenAddr = ":" + listenAddr
	}

	resolver := egress.MakeResolver(dnsServer)
	egressMgr := egress.NewManager(egressPrefer, probeURL4, probeURL6, probeInterval, resolver, proxyURL, proxyProbeInterval)
	sess := proxy.NewSessionCache(cacheFile)
	clusterNode := cluster.NewNode(clusterCfg)

	proxyCfg := proxy.Config{
		Scheme:       scheme,
		Host:         host,
		BasePath:     basePath,
		BackendURL:   backendURL,
		AuthToken:    authToken,
		InboundAuth:  inboundAuth,
		FwdInbound:   fwdInbound,
		Xff:          xff,
		Verbose:      verbose,
		Dump:         dump,
		RewriteModel: rewriteModel,
		ExtraHeaders: extraHeaders,
		Defaults:     defaults,
	}
	proxyInst := proxy.New(proxyCfg, egressMgr, sess)
	proxyInst.SetCluster(clusterNode)
	// 帧处理器直接走 proxy.handleClusterForward: 本地双栈失败后再经集群 ForwardCluster 跳下一 peer
	if err := clusterNode.Start(proxyInst.HandleClusterForward); err != nil {
		fmt.Fprintf(os.Stderr, "集群启动失败: %v\n", err)
		os.Exit(1)
	}

	handler := http.HandlerFunc(proxyInst.ServeHTTP)

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "错误: 监听 %s 失败: %v\n", listenAddr, err)
		os.Exit(1)
	}

	srv := &http.Server{
		Handler:        handler,
		ReadTimeout:    0,
		WriteTimeout:   0,
		IdleTimeout:    120 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Printf("opencode-zen-proxy 启动 (version %s):", version)
	log.Printf("  监听: %s (单端口同时接受 IPv4/IPv6)", ln.Addr())
	log.Printf("  后端: %s//%s  基础路径: %s", scheme, host, basePath)
	var egressDesc string
	if proxyURL != "" {
		egressDesc = "代理出口 (优先代理, 不区分 4/6)"
	} else {
		switch egressPrefer {
		case "d4":
			egressDesc = "强制 IPv4 (无回退)"
		case "d6":
			egressDesc = "强制 IPv6 (无回退)"
		case "4":
			egressDesc = "优先 IPv4, 失败回退 IPv6"
		case "6":
			egressDesc = "优先 IPv6, 失败回退 IPv4"
		case "auto":
			egressDesc = "并发竞速 HappyEyeballs (IPv4/IPv6 并发, 快者胜)"
		}
	}
	if proxyURL != "" {
		log.Printf("  出口策略: %s (X-Egress 头可覆盖) | 失败冷却 %s | 代理探测间隔 %s | 直连探测间隔 %s", egressDesc, egress.UnavailableCool, proxyProbeInterval, probeInterval)
	} else {
		log.Printf("  出口策略: %s (X-Egress 头可覆盖) | 失败冷却 %s | 探测间隔 %s", egressDesc, egress.UnavailableCool, probeInterval)
	}
	if clusterCfg.JoinAddr != "" || clusterCfg.ListenAddr != "" || len(clusterCfg.Peers) > 0 {
		log.Printf("  集群兜底: 双栈均失败时转发至对端 (join=%s listen=%s)", clusterCfg.JoinAddr, clusterCfg.ListenAddr)
	} else {
		log.Printf("  集群兜底: 未启用 (双栈均失败则直接返回错误, 可用 --cluster-join/--cluster-listen 启用)")
	}
log.Printf("  特征头: User-Agent=%s client=%s project=%s outbound-auth=%s", defaultUserAgent, defaultClient, defaultProject, authSummary(authToken))
	if verbose {
		util.SetLogLevel("debug")
	}
	if logLevelArg != "" {
		util.SetLogLevel(logLevelArg)
	}
	if fwdInbound && inboundAuth != "" {
		log.Printf("  入站校验: 开启 (客户端需 Authorization: Bearer %s); 转发使用同一 token (-F)", inboundAuth)
	} else if inboundAuth != "" {
		log.Printf("  入站校验: 开启 (客户端需 Authorization: Bearer %s)", inboundAuth)
	} else {
		log.Printf("  入站校验: 关闭 (默认)")
	}
	for _, fam := range []string{"6", "4"} {
		p := egressMgr.Probes[fam]
		u := probeURL6
		if fam == "4" {
			u = probeURL4
		}
		if u == "" {
			if fam == "4" {
				u = "https://api.ipify.org"
			} else {
				u = "https://api6.ipify.org"
			}
		}
		if p == nil {
			continue
		}
		if ip := p.CurrentIP(); ip != "" {
			log.Printf("  出口IP探测[%s]: %s (每 %s), 当前=%s 命名空间=%s", fam, u, probeInterval, ip, egressMgr.NsKey(fam))
		} else {
			log.Printf("  出口IP探测[%s]: %s (每 %s), 未知(退化为%s, 后台重试中)", fam, u, probeInterval, fam)
		}
	}
	if proxyURL != "" {
		if p := egressMgr.Probes["px"]; p != nil {
			if ip := p.CurrentIP(); ip != "" {
				log.Printf("  代理出口IP探测: %s (每 %s), 当前=%s 命名空间=%s", probeURL4, proxyProbeInterval, ip, egressMgr.NsKey("px"))
			} else {
				log.Printf("  代理出口IP探测: %s (每 %s), 未知(退化为px, 后台重试中)", probeURL4, proxyProbeInterval)
			}
		}
	}
	if cacheFile != "" {
		log.Printf("  会话缓存: 持久化到 %s (按 具体出口IP 隔离)", cacheFile)
	} else {
		log.Printf("  会话缓存: 仅内存 (按 具体出口IP 隔离, 不持久化)")
	}
	if clusterNode.Tunnels != nil && clusterNode.Tunnels.Path != "" {
		log.Printf("  隧道信息: 持久化到 %s (记录建立/关闭/帧数)", clusterNode.Tunnels.Path)
	}
	if clusterNode.Enabled() {
		log.Printf("  集群: id=%s listen=%s join=%s peers=%v failover-on=%v", clusterNode.SelfID(), clusterCfg.ListenAddr, clusterCfg.JoinAddr, clusterCfg.Peers, clusterCfg.FailoverOn)
	} else {
		log.Printf("  集群: 未启用 (可用 --cluster-listen/--peer/--cluster-join 启用)")
	}
	log.Printf("  示例: GET /a -> %s%s/a", backendURL, basePath)

	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Fatalf("服务异常退出: %v", err)
	}
}
