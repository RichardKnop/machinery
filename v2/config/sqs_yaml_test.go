package config_test

import (
	"github.com/RichardKnop/machinery/v2/config"
	"os"
	"path/filepath"
	"testing"
)

func TestSQSHeartbeatYAML(t *testing.T) {
	for _, tc := range []struct {
		name, keys string
		want       bool
	}{
		{"correct true", "  visibility_heartbeat: true\n", true},
		{"correct false", "  visibility_heartbeat: false\n", false},
		{"legacy true", "  visibility_hearth_beat: true\n", true},
		{"legacy false", "  visibility_hearth_beat: false\n", false},
		{"correct true wins", "  visibility_heartbeat: true\n  visibility_hearth_beat: false\n", true},
		{"correct false wins", "  visibility_heartbeat: false\n  visibility_hearth_beat: true\n", false},
		{"unset", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yml")
			data := "broker: eager\nsqs:\n  receive_wait_time_seconds: 11\n  receive_visibility_timeout: 17\n" + tc.keys
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			cnf, err := config.NewFromYaml(path, false)
			if err != nil {
				t.Fatal(err)
			}
			if cnf.SQS == nil || cnf.SQS.VisibilityHeartBeat != tc.want {
				t.Fatalf("SQS=%+v; want heartbeat %v", cnf.SQS, tc.want)
			}
			if cnf.SQS.WaitTimeSeconds != 11 || cnf.SQS.VisibilityTimeout == nil || *cnf.SQS.VisibilityTimeout != 17 {
				t.Fatalf("other SQS fields changed: %+v", cnf.SQS)
			}
		})
	}
}
