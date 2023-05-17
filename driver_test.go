package cloudwatchlogsinsightsdriver

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

var mockClients = map[string]*mockCloudWatchLogsClient{
	"none": {},
}

func init() {
	CloudwatchLogsClientConstructor = func(ctx context.Context, cfg *CloudwatchLogsInsightsConfig) (CloudwatchLogsClient, error) {
		if mockName := cfg.Params.Get("mock"); mockName != "" {
			return mockClients[mockName], nil
		}
		return DefaultCloudwatchLogsClientConstructor(ctx, cfg)
	}
	GetDebugLogger().SetOutput(os.Stderr)
}

func TestQueryContext__WithAWS__SuccessCase(t *testing.T) {
	dsn := os.Getenv("TEST_DSN")
	if dsn == "" {
		t.Skip("TEST_DSN is not set")
	}
	db, err := sql.Open("cloudwatch-logs-insights", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, "fields @timestamp, @message | limit 10",
		sql.Named("start_time", "2022-09-16T00:00:00+09:00"),
		sql.Named("end_time", "2022-09-16T23:59:59+09:00"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(columns)
	for rows.Next() {
		var timestamp time.Time
		var message string
		if err := rows.Scan(&timestamp, &message); err != nil {
			t.Fatal(err)
		}
		t.Log(timestamp, message)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestQueryRwowContext__WithMock__SuccessCase(t *testing.T) {
	c := 0
	mockClients["success_case"] = &mockCloudWatchLogsClient{
		StartQueryFunc: func(ctx context.Context, params *cloudwatchlogs.StartQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error) {
			if len(params.LogGroupNames) != 0 {
				t.Fatal("unexpected log group names length")
			}
			if params.LogGroupName == nil {
				t.Fatal("unexpected log group name")
			}
			if *params.LogGroupName != "test-log-group" {
				t.Fatal("unexpected log group name[0]")
			}
			if coalesce(params.StartTime) != 1577804400 {
				t.Log("got start_time:", *params.StartTime)
				t.Fatal("unexpected start time")
			}
			if coalesce(params.EndTime) != 1577890799 {
				t.Log("got end_time:", *params.EndTime)
				t.Fatal("unexpected end time")
			}
			return &cloudwatchlogs.StartQueryOutput{
				QueryId: aws.String("test-query-id"),
			}, nil
		},
		GetQueryResultsFunc: func(ctx context.Context, params *cloudwatchlogs.GetQueryResultsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error) {
			if c < 2 {
				c++
				return &cloudwatchlogs.GetQueryResultsOutput{
					Status: types.QueryStatusRunning,
				}, nil
			}
			return &cloudwatchlogs.GetQueryResultsOutput{
				Status: types.QueryStatusComplete,
				Results: [][]types.ResultField{
					{
						{
							Field: aws.String("@timestamp"),
							Value: aws.String("2020-01-01 00:00:01"),
						},
						{
							Field: aws.String("@message"),
							Value: aws.String("test message"),
						},
						{
							Field: aws.String("@ptr"),
							Value: aws.String("CmEKJgoiMzE0NDcyNjQzNTE1Oi9hd3MvbGFtYmRhL2xzM3ZpZXdlchAGEjUaGAIGMcwwKwAAAAAbt/SEAAYyQZYwAAABsiABKMjQkKi0MDD/25CotDA4EkDEEEicD1DGCRgAEA4YAQ=="),
						},
					},
				},
			}, nil
		},
	}
	db, err := sql.Open("cloudwatch-logs-insights", "cloudwatch://?mock=success_case&log_group_name=test-log-group")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	row := db.QueryRowContext(
		ctx, "fields @timestamp, @message | limit 1",
		sql.Named("start_time", "2020-01-01T00:00:00+09:00"),
		sql.Named("end_time", "2020-01-01T23:59:59+09:00"),
	)
	if err := row.Err(); err != nil {
		t.Fatal(err)
	}
	var timestamp time.Time
	var message string
	if err := row.Scan(&timestamp, &message); err != nil {
		t.Fatal(err)
	}
	if timestamp.Unix() != 1577836801 {
		t.Fatalf("unexpected timestamp:%s (%d)", timestamp, timestamp.Unix())
	}
	if message != "test message" {
		t.Fatal("unexpected message:", message)
	}
	if mockClients["success_case"].StartQueryCallCount != 1 {
		t.Fatal("unexpected StartQuery call count:", mockClients["success_case_with_named_parameter"].StartQueryCallCount)
	}
	if mockClients["success_case"].GetQueryResultsCallCount != 3 {
		t.Fatal("unexpected GetQueryResults call count:", mockClients["success_case_with_named_parameter"].GetQueryResultsCallCount)
	}
}

func TestQueryRowContext__WithMock__SuccessCaseWithNamedParameter(t *testing.T) {
	c := 0
	mockClients["success_case_with_named_parameter"] = &mockCloudWatchLogsClient{
		StartQueryFunc: func(ctx context.Context, params *cloudwatchlogs.StartQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error) {
			if len(params.LogGroupNames) != 2 {
				t.Fatal("unexpected log group names length")
			}
			if params.LogGroupNames[0] != "test-log-group" {
				t.Fatal("unexpected log group name[0]")
			}
			if params.LogGroupNames[1] != "test-log-group-2" {
				t.Fatal("unexpected log group name[1]")
			}
			if coalesce(params.StartTime) != 1577804400 {
				t.Log("got start_time:", *params.StartTime)
				t.Fatal("unexpected start time")
			}
			if coalesce(params.EndTime) != 1577890799 {
				t.Log("got end_time:", *params.EndTime)
				t.Fatal("unexpected end time")
			}
			return &cloudwatchlogs.StartQueryOutput{
				QueryId: aws.String("test-query-id"),
			}, nil
		},
		GetQueryResultsFunc: func(ctx context.Context, params *cloudwatchlogs.GetQueryResultsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error) {
			if c < 2 {
				c++
				return &cloudwatchlogs.GetQueryResultsOutput{
					Status: types.QueryStatusRunning,
				}, nil
			}
			return &cloudwatchlogs.GetQueryResultsOutput{
				Status: types.QueryStatusComplete,
				Results: [][]types.ResultField{
					{
						{
							Field: aws.String("@timestamp"),
							Value: aws.String("2020-01-01 00:00:01"),
						},
						{
							Field: aws.String("@message"),
							Value: aws.String("test message"),
						},
						{
							Field: aws.String("@ptr"),
							Value: aws.String("CmEKJgoiMzE0NDcyNjQzNTE1Oi9hd3MvbGFtYmRhL2xzM3ZpZXdlchAGEjUaGAIGMcwwKwAAAAAbt/SEAAYyQZYwAAABsiABKMjQkKi0MDD/25CotDA4EkDEEEicD1DGCRgAEA4YAQ=="),
						},
					},
				},
			}, nil
		},
	}
	db, err := sql.Open("cloudwatch-logs-insights", "cloudwatch://?mock=success_case_with_named_parameter")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	row := db.QueryRowContext(
		ctx, "fields @timestamp, @message | limit 1",
		sql.Named("log_group_names", "test-log-group,test-log-group-2"),
		sql.Named("start_time", "2020-01-01T00:00:00+09:00"),
		sql.Named("end_time", "2020-01-01T23:59:59+09:00"),
	)
	if err := row.Err(); err != nil {
		t.Fatal(err)
	}
	var timestamp time.Time
	var message string
	if err := row.Scan(&timestamp, &message); err != nil {
		t.Fatal(err)
	}
	if timestamp.Unix() != 1577836801 {
		t.Fatalf("unexpected timestamp:%s (%d)", timestamp, timestamp.Unix())
	}
	if message != "test message" {
		t.Fatal("unexpected message:", message)
	}
	if mockClients["success_case_with_named_parameter"].StartQueryCallCount != 1 {
		t.Fatal("unexpected StartQuery call count:", mockClients["success_case_with_named_parameter"].StartQueryCallCount)
	}
	if mockClients["success_case_with_named_parameter"].GetQueryResultsCallCount != 3 {
		t.Fatal("unexpected GetQueryResults call count:", mockClients["success_case_with_named_parameter"].GetQueryResultsCallCount)
	}
}

func TestQueryContext__WITHMock__LogGroupNameIsRequired(t *testing.T) {
	db, err := sql.Open("cloudwatch-logs-insights", "cloudwatch://?mock=none")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.QueryContext(ctx, "fields @timestamp, @message | limit 1")
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if !strings.Contains(err.Error(), "log_group_name is required") {
		t.Fatal("unexpected error:", err)
	}
}

func TestQueryContext__WITHMock__StartQueryError(t *testing.T) {
	mockClients["start_query_error"] = &mockCloudWatchLogsClient{
		StartQueryFunc: func(ctx context.Context, params *cloudwatchlogs.StartQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error) {
			return nil, errors.New("start query error")
		},
	}
	db, err := sql.Open("cloudwatch-logs-insights", "cloudwatch://?mock=start_query_error&log_group_name=test-log-group")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.QueryContext(ctx, "fields @timestamp, @message | limit 1")
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if !strings.Contains(err.Error(), "start query error") {
		t.Fatal("unexpected error message:", err.Error())
	}
	if mockClients["start_query_error"].StartQueryCallCount != 1 {
		t.Fatal("unexpected StartQuery call count:", mockClients["start_query_error"].StartQueryCallCount)
	}
	if mockClients["start_query_error"].GetQueryResultsCallCount != 0 {
		t.Fatal("unexpected GetQueryResults call count:", mockClients["start_query_error"].GetQueryResultsCallCount)
	}
}

func TestQueryContext__WITHMock__GetQueryResultsError(t *testing.T) {
	mockClients["get_query_results_error"] = &mockCloudWatchLogsClient{
		StartQueryFunc: func(ctx context.Context, params *cloudwatchlogs.StartQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error) {
			return &cloudwatchlogs.StartQueryOutput{
				QueryId: aws.String("test-query-id"),
			}, nil
		},
		GetQueryResultsFunc: func(ctx context.Context, params *cloudwatchlogs.GetQueryResultsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error) {
			return nil, errors.New("get query results error")
		},
		StopQueryFunc: func(ctx context.Context, params *cloudwatchlogs.StopQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StopQueryOutput, error) {
			return &cloudwatchlogs.StopQueryOutput{
				Success: true,
			}, nil
		},
	}
	db, err := sql.Open("cloudwatch-logs-insights", "cloudwatch://?mock=get_query_results_error")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.QueryContext(
		ctx, "fields @timestamp, @message | limit 1",
		sql.Named("log_group_name", "test-log-group"),
		sql.Named("start_time", "2020-01-01T00:00:00+09:00"),
		sql.Named("end_time", "2020-01-01T23:59:59+09:00"),
	)
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if !strings.Contains(err.Error(), "get query results error") {
		t.Fatal("unexpected error message:", err.Error())
	}
	if mockClients["get_query_results_error"].StartQueryCallCount != 1 {
		t.Fatal("unexpected StartQuery call count:", mockClients["get_query_results_error"].StartQueryCallCount)
	}
	if mockClients["get_query_results_error"].GetQueryResultsCallCount != 2 {
		t.Fatal("unexpected GetQueryResults call count:", mockClients["get_query_results_error"].GetQueryResultsCallCount)
	}
	if mockClients["get_query_results_error"].StopQueryCallCount != 1 {
		t.Fatal("unexpected StopQuery call count:", mockClients["get_query_results_error"].StopQueryCallCount)
	}
}

func TestQueryRowContext__WITHMock__Timeout(t *testing.T) {
	mockClients["timeout"] = &mockCloudWatchLogsClient{
		StartQueryFunc: func(ctx context.Context, params *cloudwatchlogs.StartQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error) {
			return &cloudwatchlogs.StartQueryOutput{
				QueryId: aws.String("test-query-id"),
			}, nil
		},
		GetQueryResultsFunc: func(ctx context.Context, params *cloudwatchlogs.GetQueryResultsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error) {
			return &cloudwatchlogs.GetQueryResultsOutput{
				Status: types.QueryStatusRunning,
			}, nil
		},
		StopQueryFunc: func(ctx context.Context, params *cloudwatchlogs.StopQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StopQueryOutput, error) {
			return &cloudwatchlogs.StopQueryOutput{
				Success: true,
			}, nil
		},
	}
	db, err := sql.Open("cloudwatch-logs-insights", "cloudwatch://?mock=timeout&timeout=1ms")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.QueryContext(
		ctx, "fields @timestamp, @message | limit 1",
		sql.Named("log_group_name", "test-log-group"),
		sql.Named("start_time", "2020-01-01T00:00:00+09:00"),
		sql.Named("end_time", "2020-01-01T23:59:59+09:00"),
	)
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if err != context.DeadlineExceeded {
		t.Fatal("unexpected error:", err)
	}
	if mockClients["timeout"].StartQueryCallCount != 1 {
		t.Fatal("unexpected StartQuery call count:", mockClients["timeout"].StartQueryCallCount)
	}
	if mockClients["timeout"].StopQueryCallCount != 1 {
		t.Fatal("unexpected StopQuery call count:", mockClients["timeout"].StopQueryCallCount)
	}
}

func TestQueryRowContext__WITHMock__QueryFailed(t *testing.T) {
	mockClients["query_failed"] = &mockCloudWatchLogsClient{
		StartQueryFunc: func(ctx context.Context, params *cloudwatchlogs.StartQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error) {
			return &cloudwatchlogs.StartQueryOutput{
				QueryId: aws.String("test-query-id"),
			}, nil
		},
		GetQueryResultsFunc: func(ctx context.Context, params *cloudwatchlogs.GetQueryResultsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error) {
			return &cloudwatchlogs.GetQueryResultsOutput{
				Status: types.QueryStatusFailed,
			}, nil
		},
	}
	db, err := sql.Open("cloudwatch-logs-insights", "cloudwatch://?mock=query_failed")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.QueryContext(
		ctx, "fields @timestamp, @message | limit 1",
		sql.Named("log_group_name", "test-log-group"),
		sql.Named("start_time", "2020-01-01T00:00:00+09:00"),
		sql.Named("end_time", "2020-01-01T23:59:59+09:00"),
	)
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if !strings.Contains(err.Error(), "query failed") {
		t.Fatal("unexpected error:", err)
	}
	if mockClients["query_failed"].StartQueryCallCount != 1 {
		t.Fatal("unexpected StartQuery call count:", mockClients["query_failed"].StartQueryCallCount)
	}
	if mockClients["query_failed"].GetQueryResultsCallCount == 0 {
		t.Fatal("unexpected GetQueryResults call count:", mockClients["query_failed"].GetQueryResultsCallCount)
	}
	if mockClients["query_failed"].StopQueryCallCount != 0 {
		t.Fatal("unexpected StopQuery call count:", mockClients["query_failed"].StopQueryCallCount)
	}
}

func TestExecContext__Unsupported(t *testing.T) {
	db, err := sql.Open("cloudwatch-logs-insights", "cloudwatch://?mock=none")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.ExecContext(ctx, "fields @timestamp, @message | limit 1")
	if err == nil {
		t.Fatal("unexpected nil error")
	}
	if !errors.Is(err, ErrNotSupported) {
		t.Fatal("unexpected error:", err)
	}
}
