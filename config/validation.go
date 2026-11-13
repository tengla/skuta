package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
)

func validateParameters(params ...string) bool {
	for _, param := range params {
		if param != "" {
			return false
		}
	}

	return true
}

func validateFileName(fileName string) error {
	_, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return fmt.Errorf("file %s dose not exist", fileName)
	}
	return nil
}

func validateURL(str string) error {
	u, err := url.Parse(str)

	if err != nil {
		return fmt.Errorf("url %s is invalid: %s", str, err.Error())
	}

	if u.Scheme == "" {
		return fmt.Errorf("url %s is invalid, no scheme", str)
	}
	if u.Host == "" {
		return fmt.Errorf("url %s is invalid, no host", str)
	}
	return nil
}

func validatePort(port int) error {
	if port <= 0 || port > 65535 {
		return fmt.Errorf("port %d is invalid", port)
	}
	return nil
}

// ParseTrustedProxies reads the addresses of reverse proxies whose
// X-Forwarded-For header a server may believe: single IPs or CIDR ranges.
func ParseTrustedProxies(values []string) ([]*net.IPNet, error) {
	var ranges []*net.IPNet
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if ip := net.ParseIP(value); ip != nil {
			bits := 128
			if ip.To4() != nil {
				ip, bits = ip.To4(), 32
			}
			ranges = append(ranges, &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)})
			continue
		}
		_, ipNet, err := net.ParseCIDR(value)
		if err != nil {
			return nil, fmt.Errorf("trusted proxy %q is not an IP address or a CIDR range", value)
		}
		ranges = append(ranges, ipNet)
	}
	return ranges, nil
}
