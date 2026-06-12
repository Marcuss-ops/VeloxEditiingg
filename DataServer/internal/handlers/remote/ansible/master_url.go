package ansible

import (
	"fmt"
	"net"
	"reflect"
	"strings"

	"github.com/gin-gonic/gin"
)

type MasterResolution struct {
	URL    string
	Source string
}

func ResolveMasterURL(cfg interface{}, c interface{}, fallback string) MasterResolution {
	if cfg != nil {
		if v, ok := extractMasterURLFromConfig(cfg); ok && v != "" {
			return MasterResolution{URL: v, Source: "config"}
		}
	}
	if c != nil {
		if v, ok := extractMasterURLFromContext(c); ok && v != "" {
			return MasterResolution{URL: v, Source: "request"}
		}
	}
	return MasterResolution{URL: fallback, Source: "fallback"}
}

func extractMasterURLFromConfig(cfg interface{}) (string, bool) {
	v := reflect.ValueOf(cfg)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "", false
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return "", false
	}
	f := v.FieldByName("MasterURL")
	if !f.IsValid() || f.Kind() != reflect.String {
		return "", false
	}
	return strings.TrimSpace(f.String()), true
}

func extractMasterURLFromContext(c interface{}) (string, bool) {
	gc, ok := c.(*gin.Context)
	if !ok || gc == nil || gc.Request == nil {
		return "", false
	}
	r := gc.Request
	host := strings.TrimSpace(r.Header.Get("X-Forwarded-Host"))
	if host == "" {
		host = strings.TrimSpace(r.Host)
	}
	if host == "" {
		return "", false
	}
	proto := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))
	if proto == "" {
		if r.TLS != nil {
			proto = "https"
		} else {
			proto = "http"
		}
	}
	return fmt.Sprintf("%s://%s", proto, host), true
}

func IsLocalhostURL(url string) bool {
	return strings.Contains(url, "localhost") || strings.Contains(url, "127.0.0.1") || strings.Contains(url, "0.0.0.0")
}

func detectLocalMasterURL() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "http://127.0.0.1:8000"
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP == nil {
			continue
		}
		ip := ipNet.IP
		if ip.IsLoopback() || ip.To4() == nil {
			continue
		}
		return fmt.Sprintf("http://%s:8000", ip.String())
	}
	return "http://127.0.0.1:8000"
}
