package utils

import (
	"github.com/labstack/echo/v4"
	"github.com/tengla/skuta/config"
)

// RemoteIPExtractor decides what echo's RealIP returns, which is the remote
// address in the audit log and in traces. Echo's default believes the
// X-Forwarded-For and X-Real-IP headers of any caller, so a client could name
// any address. Without trusted proxies only the address of the connection
// counts. With them, X-Forwarded-For is read when the connection comes from
// one of those proxies, and only back to the first address that isn't one.
func RemoteIPExtractor(trustedProxies []string) (echo.IPExtractor, error) {
	ranges, err := config.ParseTrustedProxies(trustedProxies)
	if err != nil {
		return nil, err
	}
	if len(ranges) == 0 {
		return echo.ExtractIPDirect(), nil
	}
	options := []echo.TrustOption{echo.TrustLoopback(false), echo.TrustLinkLocal(false), echo.TrustPrivateNet(false)}
	for _, ipRange := range ranges {
		options = append(options, echo.TrustIPRange(ipRange))
	}
	return echo.ExtractIPFromXFFHeader(options...), nil
}
