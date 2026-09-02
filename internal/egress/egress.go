package egress

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"opencode-zen-proxy/internal/util"
)

func MakeResolver(dnsServer string) *net.Resolver {
	if dnsServer == "" {
		return net.DefaultResolver
	}
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{Timeout: 5 * time.Second}
			return d.DialContext(ctx, "udp", net.JoinHostPort(dnsServer, "53"))
		},
	}
}

const (
	UnavailableCool    = 30 * time.Second
	MaxUnavailableCool = 60 * time.Minute
	MaxProbeInterval   = 60 * time.Minute
)

type IPProbe struct {
	mu       sync.Mutex
	mode     string
	url      string
	current  string
	trans    *http.Transport
	interval time.Duration
	failCount int
}

func newIPProbe(mode, url string, t *http.Transport, interval time.Duration) *IPProbe {
	return &IPProbe{mode: mode, url: url, trans: t, interval: interval}
}

func (p *IPProbe) CurrentIP() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}

func (p *IPProbe) setIP(ip string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.current = ip
}

func (p *IPProbe) probe() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build probe request: %w", err)
	}
	resp, err := p.trans.RoundTrip(req)
	if err != nil {
		return "", fmt.Errorf("%s network stack unavailable (dial %s failed): %w", famLabel(p.mode), p.url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
	ip := strings.TrimSpace(string(body))
	if net.ParseIP(ip) == nil {
		return "", fmt.Errorf("%s probe response has no valid IP: %q", famLabel(p.mode), body)
	}
	return ip, nil
}

func famLabel(mode string) string {
	switch mode {
	case "4":
		return "IPv4"
	case "6":
		return "IPv6"
	default:
		return "proxy"
	}
}

func (p *IPProbe) refresh() {
	ip, err := p.probe()
	if err != nil {
		p.mu.Lock()
		p.failCount++
		p.mu.Unlock()
		log.Printf("[ip-probe] %s probe failed: %v", famLabel(p.mode), err)
		return
	}
	p.mu.Lock()
	if p.current != ip {
		log.Printf("[ip-probe] %s egress IP changed: %q -> %q", famLabel(p.mode), p.current, ip)
		p.failCount = 0
	}
	p.current = ip
	p.mu.Unlock()
}

func (p *IPProbe) run() {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for range ticker.C {
		p.refresh()
		p.mu.Lock()
		fc := p.failCount
		cur := p.current
		p.mu.Unlock()
		if fc > 0 {
			shift := fc - 1
			if shift > 10 {
				shift = 10
			}
			d := UnavailableCool * time.Duration(1<<uint(shift))
			if d > MaxProbeInterval {
				d = MaxProbeInterval
			}
			if d < UnavailableCool {
				d = MaxProbeInterval
			}
			ticker.Reset(d)
			continue
		}
		if cur != "" {
			ticker.Reset(p.interval)
		}
	}
}

func makeDialContext(mode string, resolver *net.Resolver) func(ctx context.Context, network, addr string) (net.Conn, error) {
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
			addrs, err := resolver.LookupIP(ctx, "ip", host)
			if err != nil {
				return nil, fmt.Errorf("resolve %s failed: %w", host, err)
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
			lastErr = fmt.Errorf("no available %s address: %s", map[string]string{"4": "IPv4", "6": "IPv6"}[mode], host)
		}
		return nil, lastErr
	}
}

type Manager struct {
	transports   map[string]*http.Transport
	Probes       map[string]*IPProbe
	EgressPrefer string

	stackDown    map[string]bool
	stackMu      sync.Mutex
	unavail      map[string]time.Time
	unavailCount map[string]int
	unavailMu    sync.Mutex
}

func NewManager(prefer, probeURL4, probeURL6 string, interval time.Duration, resolver *net.Resolver, proxyURL string, proxyInterval time.Duration) *Manager {
	m := &Manager{
		transports:   map[string]*http.Transport{},
		Probes:       map[string]*IPProbe{},
		EgressPrefer: prefer,
		stackDown:    map[string]bool{},
		unavail:      map[string]time.Time{},
		unavailCount: map[string]int{},
	}
	if proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https" && u.Scheme != "socks5" && u.Scheme != "socks5h") {
			log.Printf("[egress] invalid proxy URL (%s), ignored, fallback to direct", proxyURL)
		} else {
			pxt := &http.Transport{
				Proxy:                 http.ProxyURL(u),
				DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: util.UpstreamTimeout,
			}
			m.transports["px"] = pxt
			if probeURL4 == "" {
				probeURL4 = "https://api.ipify.org"
			}
			if proxyInterval <= 0 {
				proxyInterval = 30 * time.Second
			}
			pp := newIPProbe("px", probeURL4, pxt, proxyInterval)
			m.Probes["px"] = pp
			if ip, err := pp.probe(); err != nil {
				log.Printf("[egress] proxy initial probe failed, namespace degraded to px, retry in background: %v", err)
			} else {
				log.Printf("[ip-probe] proxy probe succeeded, egress IP=%s", ip)
				pp.setIP(ip)
			}
			go pp.run()
			go m.recoverLoop()
			log.Printf("[egress] proxy egress enabled: %s (proxy preferred, no 4/6 distinction)", u.Redacted())
			return m
		}
	}
	transports := map[string]*http.Transport{
		"4": {DialContext: makeDialContext("4", resolver), ForceAttemptHTTP2: true, MaxIdleConns: 100, MaxIdleConnsPerHost: 100, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: util.UpstreamTimeout},
		"6": {DialContext: makeDialContext("6", resolver), ForceAttemptHTTP2: true, MaxIdleConns: 100, MaxIdleConnsPerHost: 100, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: util.UpstreamTimeout},
	}
	m.transports = transports
	if probeURL4 == "" {
		probeURL4 = "https://api.ipify.org"
	}
	if probeURL6 == "" {
		probeURL6 = "https://api6.ipify.org"
	}
	p4 := newIPProbe("4", probeURL4, transports["4"], interval)
	p6 := newIPProbe("6", probeURL6, transports["6"], interval)
	for _, p := range []*IPProbe{p4, p6} {
		if ip, err := p.probe(); err != nil {
			log.Printf("[egress] %s initial probe failed, namespace degraded to family(%s), retry in background: %v", famLabel(p.mode), p.mode, err)
		} else {
			log.Printf("[ip-probe] %s probe succeeded, egress IP=%s", famLabel(p.mode), ip)
			p.setIP(ip)
		}
		go p.run()
	}
	m.Probes = map[string]*IPProbe{"4": p4, "6": p6}
	for _, p := range []*IPProbe{p4, p6} {
		if p.CurrentIP() == "" {
			if _, err := p.probe(); err != nil && util.IsStackErrStatic(err) {
				m.stackDown[p.mode] = true
				log.Printf("[egress] %s network stack unavailable, marked down (recovery only via probe)", famLabel(p.mode))
			}
		}
	}
	go m.recoverLoop()
	return m
}

func (m *Manager) recoverLoop() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for range t.C {
		for _, p := range m.Probes {
			if p.CurrentIP() != "" {
				m.stackMu.Lock()
				if m.stackDown[p.mode] {
					delete(m.stackDown, p.mode)
					m.stackMu.Unlock()
					log.Printf("[egress] IPv%s stack recovered (probe)", p.mode)
				} else {
					m.stackMu.Unlock()
				}
			}
		}
	}
}

func (m *Manager) NsKey(fam string) string {
	if ip := m.Probes[fam].CurrentIP(); ip != "" {
		return fam + "|" + ip
	}
	return fam
}

func (m *Manager) MarkStackDown(fam string, down bool) {
	m.stackMu.Lock()
	prev := m.stackDown[fam]
	if down {
		m.stackDown[fam] = true
	} else {
		if !prev {
			m.stackMu.Unlock()
			return
		}
		delete(m.stackDown, fam)
	}
	m.stackMu.Unlock()
	if down {
		if !prev {
			log.Printf("[egress] %s network stack unavailable, marked down (recovery only via probe)", famLabel(fam))
		}
	} else {
		log.Printf("[egress] %s stack recovered", famLabel(fam))
	}
}

func (m *Manager) IsStackDown(fam string) bool {
	m.stackMu.Lock()
	defer m.stackMu.Unlock()
	return m.stackDown[fam]
}

func (m *Manager) MarkUnavailable(fam string, isStack bool) {
	if isStack {
		m.MarkStackDown(fam, true)
		return
	}
	m.unavailMu.Lock()
	c := m.unavailCount[fam] + 1
	m.unavailCount[fam] = c
	shift := c - 1
	if shift > 10 {
		shift = 10
	}
	d := UnavailableCool * time.Duration(1<<uint(shift))
	if d > MaxUnavailableCool {
		d = MaxUnavailableCool
	}
	if d < UnavailableCool {
		d = MaxUnavailableCool
	}
	m.unavail[fam] = time.Now().Add(d)
	m.unavailMu.Unlock()
	log.Printf("[egress] %s marked unavailable %s (fail %d, exponential backoff)", famLabel(fam), d, c)
}

func (m *Manager) IsUnavailable(fam string) bool {
	if m.IsStackDown(fam) {
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

func (m *Manager) MarkAvailable(fam string) {
	m.unavailMu.Lock()
	delete(m.unavail, fam)
	delete(m.unavailCount, fam)
	m.unavailMu.Unlock()
	m.MarkStackDown(fam, false)
}

func (m *Manager) EgressOrder(r *http.Request) []string {
	if m.EgressPrefer == "off" {
		return []string{}
	}
	if _, ok := m.transports["px"]; ok {
		return []string{"px"}
	}
	if v := r.Header.Get("X-Egress"); v == "4" || v == "6" {
		other := "6"
		if v == "6" {
			other = "4"
		}
		return []string{v, other}
	}
	switch m.EgressPrefer {
	case "off":
		return []string{}
	case "d4":
		return []string{"4"}
	case "d6":
		return []string{"6"}
	case "auto":
		if m.IsUnavailable("6") && !m.IsUnavailable("4") {
			return []string{"4", "6"}
		}
		if m.IsUnavailable("4") && !m.IsUnavailable("6") {
			return []string{"6", "4"}
		}
		return []string{"6", "4"}
	case "4":
		return []string{"4", "6"}
	}
	return []string{"6", "4"}
}

func (m *Manager) Transport(fam string) *http.Transport {
	return m.transports[fam]
}
