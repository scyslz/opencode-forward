package main

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	logLevel   = 1
	logLevelMu sync.Mutex
	throttleMu sync.Mutex
	throttleAt = map[string]time.Time{}
)

func init() {
	lv := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL")))
	switch lv {
	case "debug":
		logLevel = 0
	case "info", "":
		logLevel = 1
	case "warn":
		logLevel = 2
	case "error":
		logLevel = 3
	}
	if os.Getenv("VERBOSE") == "1" {
		logLevel = 0
	}
}

func setLogLevel(s string) {
	logLevelMu.Lock()
	defer logLevelMu.Unlock()
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		logLevel = 0
	case "info":
		logLevel = 1
	case "warn":
		logLevel = 2
	case "error":
		logLevel = 3
	}
}

func logDebugf(format string, args ...any) {
	if logLevel > 0 {
		return
	}
	log.Printf(format, args...)
}

func logThrottledf(key string, interval time.Duration, format string, args ...any) {
	if logLevel > 0 {
		throttleMu.Lock()
		last, ok := throttleAt[key]
		now := time.Now()
		if ok && now.Sub(last) < interval {
			throttleMu.Unlock()
			return
		}
		throttleAt[key] = now
		throttleMu.Unlock()
	}
	log.Printf(format, args...)
}

const base62 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

func secureCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func opencodeID(prefix string) string {
	var sb strings.Builder
	sb.WriteString(prefix)
	sb.WriteByte('_')
	b6 := make([]byte, 6)
	_, _ = rand.Read(b6)
	sb.WriteString(hex.EncodeToString(b6))
	b14 := make([]byte, 14)
	_, _ = rand.Read(b14)
	for _, b := range b14 {
		sb.WriteByte(base62[int(b)%len(base62)])
	}
	return sb.String()
}

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

func parseBackend(arg string) (string, string, string, error) {
	u := arg
	if !strings.Contains(arg, "://") {
		u = "http://" + arg
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

func isStackErrStatic(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "没有可用") || strings.Contains(s, "网络栈不可用") || strings.Contains(s, "network is unreachable") || strings.Contains(s, "no such host")
}
