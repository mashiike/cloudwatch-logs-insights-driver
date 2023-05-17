package cloudwatchlogsinsightsdriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("cloudwatch-logs-insights", &cloudwatchLogsInsightsDriver{})
}

type cloudwatchLogsInsightsDriver struct{}

func (d *cloudwatchLogsInsightsDriver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func (d *cloudwatchLogsInsightsDriver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &cloudwatchLogsInsightsConnector{
		d:   d,
		cfg: cfg,
	}, nil
}
