package query

import (
	"fmt"
	// "strings"
)

type AggType string

const (
	COUNT AggType = "count"
	SUM   AggType = "sum"
	AVG   AggType = "avg"
)

type Aggregate struct {
	Input   VecOperator
	Column  string
	Type    AggType
	GroupBy []string
	done    bool
}

func (a *Aggregate) Next() (*Batch, error) {

	if a.done {
		return nil, nil
	}

	if len(a.GroupBy) == 0 {

		var count int64
		var sum float64

		for {
			batch, err := a.Input.Next()

			if err != nil {
				return nil, err
			}

			if batch == nil {
				break
			}

			count += int64(batch.Size)

			if a.Type == SUM || a.Type == AVG {
				col := batch.Columns[a.Column]

				for i := 0; i < batch.Size; i++ {
					sum += toFloat(col.Data[i])
				}
			}
		}

		a.done = true

		result := &Batch{
			Columns: map[string]*Vector{},
			Size:    1,
		}

		switch a.Type {
		case COUNT:
			result.Columns["count"] = &Vector{
				Data: []any{count},
			}

		case SUM:

			result.Columns["sum"] = &Vector{
				Data: []any{sum},
			}

		case AVG:

			result.Columns["avg"] = &Vector{
				Data: []any{sum / float64(count)},
			}
		}

		return result, nil

	}

	groups := map[string]float64{}
	counts := map[string]int64{}

	for {
		batch, err := a.Input.Next()

		if err != nil {
			return nil, err
		}

		if batch == nil {
			break
		}

		groupVec := batch.Columns[a.GroupBy[0]]
		valueVec := batch.Columns[a.Column]

		for i := 0; i < batch.Size; i++ {
			key := fmt.Sprint(groupVec.Data[i])
			val := toFloat(valueVec.Data[i])

			groups[key] += val
			counts[key]++
		}

	}

	size := len(groups)

	keys := make([]any, 0, size)
	values := make([]any, 0, size)

	for k, v := range groups {

		keys = append(keys, k)

		switch a.Type {

		case SUM:
			values = append(values, v)

		case COUNT:
			values = append(values, counts[k])

		case AVG:
			values = append(values, v/float64(counts[k]))
		}
	}

	result := &Batch{
		Columns: map[string]*Vector{
			a.GroupBy[0]:   {Data: keys},
			string(a.Type): {Data: values},
		},
		Size: size,
	}

	return result, nil
}
