package main

import (
	"strings"
	"testing"
)

func TestConnectionRequiresEveryMTLSSetting(t *testing.T) {
	settings := map[string]string{
		"ENDPOINT": "127.0.0.1:7233", "NAMESPACE": "local", "TLS_SERVER_NAME": "temporal.local",
		"TLS_CA": "ca.crt", "TLS_CERT": "worker.crt", "TLS_KEY": "worker.key",
	}
	for missing := range settings {
		t.Run(missing, func(t *testing.T) {
			_, err := connectionOptions(func(key string) string {
				name := strings.TrimPrefix(key, "ANVILKIT_WORKFLOW_TEMPORAL_")
				if name == missing {
					return ""
				}
				return settings[name]
			})
			if err == nil || !strings.Contains(err.Error(), "missing Temporal setting "+missing) {
				t.Fatal("missing mTLS configuration did not fail closed", err)
			}
		})
	}
}
