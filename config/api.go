package config

type ApiConfig struct {
	Port int `json:"port"`
	// TrustedProxies lists the reverse proxies (IPs or CIDR ranges) whose
	// X-Forwarded-For header names the client. Empty: the address of the
	// connection is the client, and the header is ignored.
	TrustedProxies []string `json:"trustedProxies"`
}

func defaultApiConfig() *ApiConfig {
	bindViperEnv(
		"Api.Port",
		"Api.TrustedProxies",
	)
	return &ApiConfig{
		Port: 8080,
	}
}

func (a *ApiConfig) Validate() error {
	if err := validatePort(a.Port); err != nil {
		return NewConfigurationErrorf("bad API config: %s", err.Error())
	}
	if _, err := ParseTrustedProxies(a.TrustedProxies); err != nil {
		return NewConfigurationErrorf("bad API config: %s", err.Error())
	}
	return nil
}
