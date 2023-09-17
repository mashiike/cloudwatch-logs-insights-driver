package cloudwatchlogsinsightsdriver

import (
	"errors"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

// CloudwatchLogsConfig is the configuration for the Cloudwatch Logs Insights.
type CloudwatchLogsInsightsConfig struct {
	OptFns        []func(*cloudwatchlogs.Options)
	Timeout       time.Duration // Default: 10s
	Polling       time.Duration // Default: 100ms
	LogGroupNames []string
	Region        string
	Limit         *int32

	Params url.Values
}

// ParseDSN is the function that constructs Config from the given DSN.
// For example, there is the following example.
// cloudwatch://?log_group_names=/aws/lambda/hoge,/aws/lambda/bar&limit=100&polling=1s&timeout=10s&region=ap-northeast-1
// In this case, the following Config is constructed.
//
//	CloudwatchLogsInsightsConfig{
//		OptFns:        []func(*cloudwatchlogs.Options){},
//		Timeout:       10s,
//		Polling:       1s,
//		LogGroupNames: []string{"/aws/lambda/hoge", "/aws/lambda/bar"},
//		Region:        "ap-northeast-1",
//		Limit:         100,
//	}
//
// Also, you can specify log_group_name instead of log_group_names.
// However, you can not specify log_group_name and log_group_names at the same time.
func ParseDSN(dsn string) (*CloudwatchLogsInsightsConfig, error) {
	cfg := &CloudwatchLogsInsightsConfig{
		OptFns: []func(*cloudwatchlogs.Options){},
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "cloudwatch" {
		return nil, ErrInvalidScheme
	}
	if u.Host != "" {
		return nil, errors.New("can not set host")
	}
	if u.User != nil {
		return nil, errors.New("can not set user")
	}
	if u.Path != "" {
		return nil, errors.New("can not set path")
	}
	q := u.Query()
	if v := q.Get("region"); v != "" {
		cfg.Region = v
		q.Del("region")
	} else {
		cfg.Region = os.Getenv("AWS_REGION")
	}
	if v := q.Get("timeout"); v != "" {
		if cfg.Timeout, err = time.ParseDuration(v); err != nil {
			return nil, err
		}
		q.Del("timeout")
	} else {
		cfg.Timeout = 10 * time.Second
	}
	if v := q.Get("polling"); v != "" {
		if cfg.Polling, err = time.ParseDuration(v); err != nil {
			return nil, err
		}
		q.Del("polling")
	} else {
		cfg.Polling = 100 * time.Millisecond
	}
	if v := q.Get("limit"); v != "" {
		i, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return nil, err
		}
		cfg.Limit = nullif(int32(i))
		q.Del("limit")
	} else {
		cfg.Limit = nil
	}
	if v := q.Get("log_group_names"); v != "" {
		cfg.LogGroupNames = strings.Split(v, ",")
		q.Del("log_group_names")
	}
	if v := q.Get("log_group_name"); v != "" {
		if len(cfg.LogGroupNames) > 0 {
			return nil, errors.New("can not set log_group_name and log_group_names at the same time")
		}
		cfg.LogGroupNames = []string{v}
		q.Del("log_group_name")
	}
	cfg.Params = q
	return cfg, nil
}

func (cfg *CloudwatchLogsInsightsConfig) String() string {
	values := make(url.Values)
	for k, v := range cfg.Params {
		values[k] = v
	}
	if cfg.Region != "" {
		values.Set("region", cfg.Region)
	}
	if cfg.Timeout != 0 {
		values.Set("timeout", cfg.Timeout.String())
	}
	if cfg.Polling != 0 {
		values.Set("polling", cfg.Polling.String())
	}
	if cfg.Limit != nil {
		values.Set("limit", strconv.FormatInt(int64(*cfg.Limit), 10))
	}
	if len(cfg.LogGroupNames) > 0 {
		if len(cfg.LogGroupNames) == 1 {
			values.Set("log_group_name", cfg.LogGroupNames[0])
		} else {
			values.Set("log_group_names", strings.Join(cfg.LogGroupNames, ","))
		}
	}
	return "cloudwatch://?" + values.Encode()
}
