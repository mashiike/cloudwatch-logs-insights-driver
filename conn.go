package cloudwatchlogsinsightsdriver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

type cloudwatchLogsInsightsConn struct {
	client   CloudwatchLogsClient
	cfg      *CloudwatchLogsInsightsConfig
	aliveCh  chan struct{}
	isClosed bool
}

func newConn(client CloudwatchLogsClient, cfg *CloudwatchLogsInsightsConfig) *cloudwatchLogsInsightsConn {
	return &cloudwatchLogsInsightsConn{
		client:  client,
		cfg:     cfg,
		aliveCh: make(chan struct{}),
	}
}

func (conn *cloudwatchLogsInsightsConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepared statment %w", ErrNotSupported)
}

func (conn *cloudwatchLogsInsightsConn) Prepare(query string) (driver.Stmt, error) {
	return conn.PrepareContext(context.Background(), query)
}

func (conn *cloudwatchLogsInsightsConn) Close() error {
	if conn.isClosed {
		return nil
	}
	conn.isClosed = true
	close(conn.aliveCh)
	return nil
}

func (conn *cloudwatchLogsInsightsConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, fmt.Errorf("transaction %w", ErrNotSupported)
}

func (conn *cloudwatchLogsInsightsConn) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

func (conn *cloudwatchLogsInsightsConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	endTime := time.Now()
	startTime := endTime.Add(-15 * time.Minute)
	var logGroupNames []string
	limit := conn.cfg.Limit
	var logGroupName *string
	var err error
	for _, arg := range args {
		switch arg.Name {
		case "start_time":
			switch v := arg.Value.(type) {
			case time.Time:
				startTime = v
			case string:
				startTime, err = time.Parse(time.RFC3339, v)
				if err != nil {
					return nil, fmt.Errorf("start_time cannot be parsed: %w", err)
				}
			default:
				return nil, fmt.Errorf("start_time must be time.Time or string")
			}
		case "end_time":
			switch v := arg.Value.(type) {
			case time.Time:
				endTime = v
			case string:
				endTime, err = time.Parse(time.RFC3339, v)
				if err != nil {
					return nil, fmt.Errorf("end_time cannot be parsed: %w", err)
				}
			default:
				return nil, fmt.Errorf("end_time must be time.Time or string")
			}
		case "log_group_name":
			logGroupNames = append(logGroupNames, arg.Value.(string))
		case "log_group_names":
			switch v := arg.Value.(type) {
			case []string:
				logGroupNames = append(logGroupNames, v...)
			case string:
				logGroupNames = append(logGroupNames, strings.Split(v, ",")...)
			default:
				return nil, fmt.Errorf("log_group_names must be []string or string")
			}
		case "limit":
			limit = aws.Int32(int32(arg.Value.(int)))
		}
	}
	if len(logGroupNames) == 0 {
		if len(conn.cfg.LogGroupNames) == 0 {
			return nil, fmt.Errorf("log_group_name is required")
		}
		logGroupNames = conn.cfg.LogGroupNames
	}
	if len(logGroupNames) == 1 {
		logGroupName = aws.String(logGroupNames[0])
		logGroupNames = nil
	}
	params := &cloudwatchlogs.StartQueryInput{
		QueryString:   nullif(query),
		StartTime:     aws.Int64(startTime.Unix()),
		EndTime:       aws.Int64(endTime.Unix()),
		Limit:         limit,
		LogGroupNames: logGroupNames,
		LogGroupName:  logGroupName,
	}
	output, err := conn.startQuery(ctx, params)
	if err != nil {
		return nil, err
	}
	return newRows(output), nil
}

func (conn *cloudwatchLogsInsightsConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return nil, fmt.Errorf("exec statment %w", ErrNotSupported)
}

func (conn *cloudwatchLogsInsightsConn) startQuery(ctx context.Context, params *cloudwatchlogs.StartQueryInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
	debugLogger.Printf("query: %s", coalesce(params.QueryString))
	ectx, cancel := context.WithTimeout(ctx, conn.cfg.Timeout)
	defer cancel()
	startQueryOutput, err := conn.client.StartQuery(ectx, params)
	if err != nil {
		return nil, fmt.Errorf("start query:%w", err)
	}
	queryStart := time.Now()
	logPrefix := coalesce(startQueryOutput.QueryId, nullif("-"))
	debugLogger.Printf("[%s] start query statement: %s", logPrefix, coalesce(params.QueryString))
	var isFinished bool
	defer func() {
		if !isFinished {
			getQueryResultsOutput, err := conn.client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{
				QueryId: startQueryOutput.QueryId,
			})
			if err != nil {
				debugLogger.Printf("[%s] failed get query results for finish: %v", logPrefix, err)
			} else {
				switch getQueryResultsOutput.Status {
				case types.QueryStatusCancelled, types.QueryStatusFailed, types.QueryStatusComplete, types.QueryStatusTimeout:
					// no need cancel
					return
				}
			}
			debugLogger.Printf("[%s] try stop query", logPrefix)
			output, err := conn.client.StopQuery(ctx, &cloudwatchlogs.StopQueryInput{
				QueryId: startQueryOutput.QueryId,
			})
			if err != nil {
				errLogger.Printf("[%s] failed stop query: %v", logPrefix, err)
				return
			}
			if !output.Success {
				debugLogger.Printf("[%s] stop query is not success", logPrefix)
			}
		}
	}()
	getQueryResultsOutput, err := conn.client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{
		QueryId: startQueryOutput.QueryId,
	})
	if err != nil {
		return nil, fmt.Errorf("get query results:%w", err)
	}
	delay := time.NewTimer(conn.cfg.Polling)
	for {
		if getQueryResultsOutput.Status == types.QueryStatusComplete {
			break
		}
		if getQueryResultsOutput.Status == types.QueryStatusFailed {
			return nil, fmt.Errorf("query failed: %s", coalesce(startQueryOutput.QueryId))
		}
		debugLogger.Printf("[%s] wating finsih query: elapsed_time=%s", logPrefix, time.Since(queryStart))
		delay.Reset(conn.cfg.Polling)
		select {
		case <-ectx.Done():
			if !delay.Stop() {
				<-delay.C
			}
			return nil, ectx.Err()
		case <-delay.C:
		case <-conn.aliveCh:
			if !delay.Stop() {
				<-delay.C
			}
			return nil, ErrConnClosed
		}
		getQueryResultsOutput, err = conn.client.GetQueryResults(ctx, &cloudwatchlogs.GetQueryResultsInput{
			QueryId: startQueryOutput.QueryId,
		})
		if err != nil {
			return nil, fmt.Errorf("get query results:%w", err)
		}
	}
	isFinished = true
	debugLogger.Printf("[%s] success query: elapsed_time=%s", logPrefix, time.Since(queryStart))
	debugLogger.Printf("[%s] query has result set: result_rows=%d", logPrefix, len(getQueryResultsOutput.Results))
	return getQueryResultsOutput, nil
}
