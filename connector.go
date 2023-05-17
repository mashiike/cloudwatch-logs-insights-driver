package cloudwatchlogsinsightsdriver

import (
	"context"
	"database/sql/driver"
)

type cloudwatchLogsInsightsConnector struct {
	d   *cloudwatchLogsInsightsDriver
	cfg *CloudwatchLogsInsightsConfig
}

func (c *cloudwatchLogsInsightsConnector) Connect(ctx context.Context) (driver.Conn, error) {
	client, err := newCloudwatchLogsClientClient(ctx, c.cfg)
	if err != nil {
		return nil, err
	}
	return newConn(client, c.cfg), nil
}

func (c *cloudwatchLogsInsightsConnector) Driver() driver.Driver {
	return c.d
}
