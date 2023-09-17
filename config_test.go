package cloudwatchlogsinsightsdriver

import (
	"testing"
	"time"
)

func TestConfgParseDSN(t *testing.T) {
	cfg := CloudwatchLogsInsightsConfig{
		LogGroupNames: []string{"log-group-name", "log-group-name-2"},
		Region:        string("region"),
		Timeout:       time.Duration(10 * time.Second),
		Polling:       time.Duration(100 * time.Millisecond),
	}
	dsn := cfg.String()
	t.Log(dsn)
	cfg2, err := ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.Region != cfg.Region {
		t.Errorf("expected %q, got %q", cfg.Region, cfg2.Region)
	}
	if cfg2.Timeout != cfg.Timeout {
		t.Errorf("expected %q, got %q", cfg.Timeout, cfg2.Timeout)
	}
	if cfg2.Polling != cfg.Polling {
		t.Errorf("expected %q, got %q", cfg.Polling, cfg2.Polling)
	}
	if len(cfg2.LogGroupNames) != len(cfg.LogGroupNames) {
		t.Errorf("expected %q, got %q", cfg.LogGroupNames, cfg2.LogGroupNames)
	}
	for i, v := range cfg.LogGroupNames {
		if cfg2.LogGroupNames[i] != v {
			t.Errorf("expected %q, got %q", v, cfg2.LogGroupNames[i])
		}
	}
}
