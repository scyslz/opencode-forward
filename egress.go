package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	unavailableCool     = 30 * time.Second
	maxUnavailableCool  = 10 * time.Minute
)

type ipProbe struct {
	mu       sync.Mutex
	mode     string
	url      string
	current  string
	trans    *http.Transport
	interval time.Duration
	failCount int
}

func newIPProbe(mode, url string, t *http.Transport, interval time.Duration) *ipProbe {
	return &ipProbe{mode: mode, url: url, trans: t, interval: interval}
}

func (p *ipProbe) currentIP() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}

func (p *ipProbe) setIP(ip string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = ip
}

func (p *ipProbe) probe() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		return "", fmt.Errorf("构造探测请求失败: %w", err)
	}
	resp, err := p.trans.RoundTrip(req)
	if err != nil {
		return "", fmt.Errorf("IPv%s 网络栈不可用 (连接 %s 失败): %w", p.mode, p.url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
	ip := strings.TrimSpace(string(body))
	if net.ParseIP(ip) == nil {
		return "", fmt.Errorf("IPv%s 探测响应无有效 IP: %q", p.mode, body)
	}
	return ip, nil
}

func (p *ipProbe) refresh() {
	ip, err := p.probe()
	if err != nil {
		p.mu.Lock()
		p.failCount++
		p.mu.Unlock()
		log.Printf("[ip-probe] IPv%s 探测失败: %v", p.mode, err)
		return
	}
	p.mu.Lock()
	if p.current != ip {
		log.Printf("[ip-probe] IPv%s 出口IP变化: %q -> %q", p.mode, p.current, ip)
		p.failCount = 0
	}
	p.current = ip
	p.mu.Unlock()
}

func (p *ipProbe) run() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for range ticker.C {
		p.refresh()
		p.mu.Lock()
		fc := p.failCount
		cur := p.current
		p.mu.Unlock()
		if fc > 0 {
			d := unavailableCool * time.Duration(1<<uint(fc-1))
			if d > maxUnavailableCool {
				d = maxUnavailableCool
			}
			ticker.Reset(d)
			continue
		}
		if cur != "" {
			ticker.Reset(p.interval)
		}
	}
}

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

type egressManager struct {
	transports  map[string]*http.Transport
	probes      map[string]*ipProbe
	egressPrefer string

	stackDown   map[string]bool
	stackMu     sync.Mutex
	unavail     map[string]time.Time
	unavailMu   sync.Mutex
}

func newEgressManager(prefer string, probeURL4, probeURL6 string, interval time.Duration) *egressManager {
	transports := map[string]*http.Transport{
		"4": {DialContext: makeDialContext("4"), ForceAttemptHTTP2: true, MaxIdleConns: 100, MaxIdleConnsPerHost: 100, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second},
		"6": {DialContext: makeDialContext("6"), ForceAttemptHTTP2: true, MaxIdleConns: 100, MaxIdleConnsPerHost: 100, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second},
	}
	if probeURL4 == "" {
		probeURL4 = "https://api.ipify.org"
	}
	if probeURL6 == "" {
		probeURL6 = "https://api6.ipify.org"
	}
	p4 := newIPProbe("4", probeURL4, transports["4"], interval)
	p6 := newIPProbe("6", probeURL6, transports["6"], interval)
	for _, p := range []*ipProbe{p4, p6} {
		if ip, err := p.probe(); err != nil {
			log.Printf("警告: IPv%s 启动探测出口IP失败, 命名空间退化为家族(%s), 后台自动重试: %v", p.mode, p.mode, err)
		} else {
			log.Printf("[ip-probe] IPv%s 启动检测成功, 当前出口IP=%s", p.mode, ip)
			p.setIP(ip)
		}
		go p.run()
	}
	m := &egressManager{
		transports:   transports,
		probes:       map[string]*ipProbe{"4": p4, "6": p6},
		egressPrefer: prefer,
		stackDown:    map[string]bool{},
		unavail:      map[string]time.Time{},
	}
	for _, p := range []*ipProbe{p4, p6} {
		if p.currentIP() == "" {
			if _, err := p.probe(); err != nil && isStackErrStatic(err) {
				m.stackDown[p.mode] = true
				log.Printf("[egress] IPv%s 网络栈不可用, 已标记栈不可用(仅由探测恢复, 不重试)", p.mode)
			}
		}
	}
	go m.recoverLoop()
	return m
}

func (m *egressManager) recoverLoop() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for range t.C {
		for _, p := range m.probes {
			if p.currentIP() != "" {
				m.stackMu.Lock()
				if m.stackDown[p.mode] {
					delete(m.stackDown, p.mode)
					m.stackMu.Unlock()
					log.Printf("[egress] IPv%s 网络栈已恢复(探测)", p.mode)
				} else {
					m.stackMu.Unlock()
				}
			}
		}
	}
}

func (m *egressManager) nsKey(fam string) string {
	if ip := m.probes[fam].currentIP(); ip != "" {
		return fam + "|" + ip
	}
	return fam
}

func (m *egressManager) markStackDown(fam string, down bool) {
	m.stackMu.Lock()
	m.stackDown[fam] = down
	if !down {
		delete(m.stackDown, fam)
	}
	m.stackMu.Unlock()
	if down {
		log.Printf("[egress] IPv%s 网络栈不可用, 已标记栈不可用(仅由探测恢复, 不重试)", fam)
	} else {
		log.Printf("[egress] IPv%s 网络栈已恢复", fam)
	}
}

func (m *egressManager) isStackDown(fam string) bool {
	m.stackMu.Lock()
	defer m.stackMu.Unlock()
	return m.stackDown[fam]
}

func (m *egressManager) markUnavailable(fam string, isStack bool) {
	if isStack {
		m.markStackDown(fam, true)
		return
	}
	m.unavailMu.Lock()
	m.unavail[fam] = time.Now().Add(unavailableCool)
	m.unavailMu.Unlock()
	log.Printf("[egress] IPv%s 标记不可用 %s", fam, unavailableCool)
}

func (m *egressManager) isUnavailable(fam string) bool {
	if m.isStackDown(fam) {
		return true
	}
	m.unavailMu.Lock()
	defer m.unavailMu.Unlock()
	if t, ok := m.unavail[fam]; ok {
		if time.Now().Before(t) {
			return true
		}
		delete(m.unavail, fam)
	}
	return false
}

func (m *egressManager) markAvailable(fam string) {
	m.unavailMu.Lock()
	delete(m.unavail, fam)
	m.unavailMu.Unlock()
	m.markStackDown(fam, false)
}

func (m *egressManager) egressOrder(r *http.Request) []string {
	if v := r.Header.Get("X-Egress"); v == "4" || v == "6" {
		other := "6"
		if v == "6" {
			other = "4"
		}
		return []string{v, other}
	}
	switch m.egressPrefer {
	case "d4":
		return []string{"4"}
	case "d6":
		return []string{"6"}
	case "auto":
		if m.isUnavailable("6") && !m.isUnavailable("4") {
			return []string{"4", "6"}
		}
		if m.isUnavailable("4") && !m.isUnavailable("6") {
			return []string{"6", "4"}
		}
		return []string{"6", "4"}
	case "4":
		return []string{"4", "6"}
	}
	return []string{"6", "4"}
}

func (m *egressManager) transport(fam string) *http.Transport {
	return m.transports[fam]
}
