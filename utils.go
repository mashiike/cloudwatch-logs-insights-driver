package cloudwatchlogsinsightsdriver

func nullif[T comparable](v T) *T {
	var empty T
	if v == empty {
		return nil
	}
	return &v
}

func coalesce[T any](vals ...*T) T {
	for _, val := range vals {
		if val != nil {
			return *val
		}
	}
	var empty T
	return empty
}
