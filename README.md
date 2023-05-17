# cloudwatch-logs-insights-driver

[![Documentation](https://godoc.org/github.com/mashiike/cloudwatch-logs-insights-driver?status.svg)](https://godoc.org/github.com/mashiike/cloudwatch-logs-insights-driver)
![Latest GitHub tag](https://img.shields.io/github/tag/mashiike/cloudwatch-logs-insights-driver.svg)
![Github Actions test](https://github.com/mashiike/cloudwatch-logs-insights-driver/workflows/Test/badge.svg?branch=main)
[![Go Report Card](https://goreportcard.com/badge/mashiike/cloudwatch-logs-insights-driver)](https://goreportcard.com/report/mashiike/cloudwatch-logs-insights-driver)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mashiike/cloudwatch-logs-insights-driver/blob/master/LICENSE)

 Cloudwatch Logs Insights Driver for Go's [database/sql](https://pkg.go.dev/database/sql) package

# Usage 

for example:

```go 
package main

import(
	"context"
	"database/sql"
	"log"
    "time"

	_ "github.com/mashiike/cloudwatch-logs-insights-driver"
)

func main() {
	db, err := sql.Open("cloudwatch-logs-insights", "cloudwatch://?timeout=1m")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(
		context.Background(),
		`fields @timestamp, @message | limit 10`,
        sql.Named("log_group_names", "test-log-group,test-log-group-2"),
		sql.Named("start_time", "2020-01-01T00:00:00+09:00"),
		sql.Named("end_time", "2020-01-01T23:59:59+09:00"),
	)
	if err != nil {
		log.Fatalln(err)
	}
    defer rows.Close()
	for rows.Next() {
        var timestamp time.Time
		var message string
		err := rows.Scan(&timestamp, &message)
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf("%s\t%s", teimstamp, message)
	}
}
```

## LICENSE

MIT


