package cloudwatchlogsinsightsdriver

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

type mockCloudWatchLogsClient struct {
	StartQueryCallCount      int
	StartQueryFunc           func(ctx context.Context, params *cloudwatchlogs.StartQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error)
	GetQueryResultsCallCount int
	GetQueryResultsFunc      func(ctx context.Context, params *cloudwatchlogs.GetQueryResultsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error)
	StopQueryCallCount       int
	StopQueryFunc            func(ctx context.Context, params *cloudwatchlogs.StopQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StopQueryOutput, error)
}

func (m *mockCloudWatchLogsClient) StartQuery(ctx context.Context, params *cloudwatchlogs.StartQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error) {
	m.StartQueryCallCount++
	if m.StartQueryFunc == nil {
		return nil, fmt.Errorf("unexpected call to StartQueryFunc")
	}
	return m.StartQueryFunc(ctx, params, optFns...)
}

func (m *mockCloudWatchLogsClient) GetQueryResults(ctx context.Context, params *cloudwatchlogs.GetQueryResultsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error) {
	m.GetQueryResultsCallCount++
	if m.GetQueryResultsFunc == nil {
		return nil, fmt.Errorf("unexpected call to GetQueryResultsFunc")
	}
	return m.GetQueryResultsFunc(ctx, params, optFns...)
}

func (m *mockCloudWatchLogsClient) StopQuery(ctx context.Context, params *cloudwatchlogs.StopQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StopQueryOutput, error) {
	m.StopQueryCallCount++
	if m.StopQueryFunc == nil {
		return nil, fmt.Errorf("unexpected call to StopQueryFunc")
	}
	return m.StopQueryFunc(ctx, params, optFns...)
}
