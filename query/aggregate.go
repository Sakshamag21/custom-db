package query

import (
	"fmt"
	"strings"
)

type AggType string

const (
	COUNT AggType = "count"
	SUM   AggType = "sum"
	AVG   AggType = "avg"
)

type Aggregate struct {
	Input   Operator
	Column  string
	Type    AggType
	GroupBy []string
}

func (a *Aggregate) Execute() ([]Row, error) {
	rows, err := a.Input.Execute()

	if err != nil {
		return nil, err
	}

	if len(a.GroupBy) == 0 {
		switch a.Type {
		case COUNT:
			return []Row{
				{"count": len(rows)},
			}, nil

		case SUM:
			var sum int64
			for _, r := range rows {
				sum += r[a.Column].(int64)
			}

			return []Row{
				{"sum": sum},
			}, nil

		case AVG:
			var sum int64
			for _, r := range rows {
				sum += r[a.Column].(int64)
			}

			return []Row{
				{"avg": sum / int64(len(rows))},
			}, nil
		}

	}

	groups := make(map[string][]Row)

	for _, r := range rows {
		var keyParts []string
		for _, col := range a.GroupBy {
			keyParts = append(keyParts, fmt.Sprint(r[col]))
		}

		key := strings.Join(keyParts, "|")
		groups[key] = append(groups[key], r)
	}

	var result []Row

	for key, groupRows := range groups {
		out := make(Row)

		parts := strings.Split(key, "|")

		for i, col := range a.GroupBy {
			out[col] = parts[i]
		}

		switch a.Type {
		case COUNT:
			out["count"] = len(groupRows)
		case SUM:
			var sum int64
			for _, r := range groupRows {
				sum += r[a.Column].(int64)
			}
			out["sum"] = sum
		case AVG:
			var sum int64
			for _, r := range groupRows {
				sum += r[a.Column].(int64)
			}
			out["avg"] = sum / int64(len(groupRows))
		}

		result = append(result, out)

	}

	return result, nil

}
