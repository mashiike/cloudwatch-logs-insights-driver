package cloudwatchlogsinsightsdriver

import (
	"database/sql/driver"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

type cloudWatchLogsInsightsRows struct {
	columns []string
	rows    [][]driver.Value
	index   int
}

func (r *cloudWatchLogsInsightsRows) Columns() []string {
	return r.columns
}

func (r *cloudWatchLogsInsightsRows) Close() error {
	return nil
}

func (r *cloudWatchLogsInsightsRows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return io.EOF
	}

	row := r.rows[r.index]
	r.index++
	if len(row) != len(dest) {
		return io.ErrShortBuffer
	}
	copy(dest, row)

	return nil
}

func newRows(output *cloudwatchlogs.GetQueryResultsOutput) *cloudWatchLogsInsightsRows {
	results := output.Results
	if len(results) == 0 {
		return &cloudWatchLogsInsightsRows{
			columns: make([]string, 0),
			rows:    make([][]driver.Value, 0),
			index:   0,
		}
	}
	columns := make([]string, 0, len(results[0]))
	index := make(map[int]int, len(results[0]))
	for i, field := range results[0] {
		column := aws.ToString(field.Field)
		if column == "@ptr" {
			continue
		}
		index[i] = len(columns)
		columns = append(columns, column)
	}

	rows := make([][]driver.Value, len(results))
	for i := 0; i < len(results); i++ {
		rowValues := make([]driver.Value, len(columns))
		for j, field := range results[i] {
			name := aws.ToString(field.Field)
			if name == "@ptr" {
				continue
			}
			str := aws.ToString(field.Value)
			if name == "@timestamp" {
				if t, err := time.Parse("2006-01-02 15:04:05", str); err == nil {
					rowValues[index[j]] = driver.Value(t)
					continue
				}
			}
			rowValues[index[j]] = driver.Value(str)
		}
		rows[i] = rowValues
	}

	return &cloudWatchLogsInsightsRows{
		columns: columns,
		rows:    rows,
		index:   0,
	}
}
