package config

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
)

func TestRedisCredentialsProgrammaticOnly(t *testing.T) {
	cfg := &RedisConfig{CredentialsProviderContext: func(context.Context) (string, string, error) { return "synthetic-user", "synthetic-password", nil }}
	for name, marshal := range map[string]func(interface{}) ([]byte, error){"yaml": yaml.Marshal, "json": json.Marshal} {
		data, err := marshal(cfg)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.Contains(strings.ToLower(string(data)), "credentialsprovidercontext") {
			t.Errorf("%s serialized the callback", name)
		}
	}
	t.Setenv("REDIS_CREDENTIALS_PROVIDER_CONTEXT", "not-a-function")
	cfgFromEnv, err := NewFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if cfgFromEnv.Redis.CredentialsProviderContext != nil {
		t.Error("environment populated a callback")
	}
}
