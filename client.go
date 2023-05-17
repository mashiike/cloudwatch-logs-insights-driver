package cloudwatchlogsinsightsdriver

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

// CloudwatchLogsClient is the interface for the Cloudwatch Logs Insights client.
type CloudwatchLogsClient interface {
	StartQuery(ctx context.Context, params *cloudwatchlogs.StartQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error)
	GetQueryResults(ctx context.Context, params *cloudwatchlogs.GetQueryResultsInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error)
	StopQuery(ctx context.Context, params *cloudwatchlogs.StopQueryInput, optFns ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StopQueryOutput, error)
}

// 　CloudwatchLogsClientConstructor is the constructor for the Cloudwatch Logs Insights client.
var CloudwatchLogsClientConstructor func(ctx context.Context, cfg *CloudwatchLogsInsightsConfig) (CloudwatchLogsClient, error)

func newCloudwatchLogsClientClient(ctx context.Context, cfg *CloudwatchLogsInsightsConfig) (CloudwatchLogsClient, error) {
	if CloudwatchLogsClientConstructor != nil {
		return CloudwatchLogsClientConstructor(ctx, cfg)
	}
	return DefaultCloudwatchLogsClientConstructor(ctx, cfg)
}

// DefaultCloudwatchLogsClientConstructor is the default constructor for the Cloudwatch Logs Insights client.
func DefaultCloudwatchLogsClientConstructor(ctx context.Context, cfg *CloudwatchLogsInsightsConfig) (CloudwatchLogsClient, error) {
	optFns := []func(*config.LoadOptions) error{}
	if cfg.Region != "" {
		optFns = append(optFns, config.WithRegion(cfg.Region))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, err
	}
	client := cloudwatchlogs.NewFromConfig(awsCfg, cfg.OptFns...)
	return client, nil
}
