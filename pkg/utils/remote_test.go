package utils

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteIPExtractor(t *testing.T) {
	request := func(remoteAddr string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.RemoteAddr = remoteAddr
		req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.5")
		req.Header.Set("X-Real-IP", "203.0.113.8")
		return req
	}

	t.Run("no trusted proxies: headers are ignored", func(t *testing.T) {
		extract, err := RemoteIPExtractor(nil)
		require.NoError(t, err)
		require.Equal(t, "10.0.0.5", extract(request("10.0.0.5:4000")))
		require.Equal(t, "127.0.0.1", extract(request("127.0.0.1:4000")))
	})
	t.Run("trusted proxy: the forwarded client is used", func(t *testing.T) {
		extract, err := RemoteIPExtractor([]string{"10.0.0.0/8"})
		require.NoError(t, err)
		require.Equal(t, "203.0.113.7", extract(request("10.0.0.5:4000")))
	})
	t.Run("connection from elsewhere: headers are ignored", func(t *testing.T) {
		extract, err := RemoteIPExtractor([]string{"10.0.0.5"})
		require.NoError(t, err)
		require.Equal(t, "198.51.100.1", extract(request("198.51.100.1:4000")))
		require.Equal(t, "127.0.0.1", extract(request("127.0.0.1:4000")))
	})
	t.Run("invalid entry", func(t *testing.T) {
		_, err := RemoteIPExtractor([]string{"not-an-address"})
		require.Error(t, err)
	})
}
