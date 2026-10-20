package config_test

import (
	"os"
	"testing"

	"github.com/RichardKnop/machinery/v2/config"
)

func TestMultipleBrokerSeparatorEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name              string
		canonical, legacy *string
		want              string
	}{
		{name: "canonical", canonical: separatorValue(";"), want: ";"},
		{name: "legacy", legacy: separatorValue("|"), want: "|"},
		{name: "canonical wins", canonical: separatorValue(";"), legacy: separatorValue("|"), want: ";"},
		{name: "canonical empty wins", canonical: separatorValue(""), legacy: separatorValue("|"), want: ""},
		{name: "legacy empty", legacy: separatorValue(""), want: ""},
		{name: "unset", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range []string{"MULTIPLE_BROKER_SEPARATOR", "MULTIPLE_BROKEN_SEPARATOR"} {
				t.Setenv(key, "")
				if err := os.Unsetenv(key); err != nil {
					t.Fatal(err)
				}
			}
			t.Setenv("BROKER", "amqp://first;amqp://second")
			if tc.canonical != nil {
				t.Setenv("MULTIPLE_BROKER_SEPARATOR", *tc.canonical)
			}
			if tc.legacy != nil {
				t.Setenv("MULTIPLE_BROKEN_SEPARATOR", *tc.legacy)
			}
			cnf, err := config.NewFromEnvironment()
			if err != nil {
				t.Fatal(err)
			}
			if cnf.MultipleBrokerSeparator != tc.want {
				t.Fatalf("separator=%q; want %q", cnf.MultipleBrokerSeparator, tc.want)
			}
		})
	}
}

func separatorValue(value string) *string { return &value }
