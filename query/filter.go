package query

import (
	"fmt"
)

type FilterFunc func(Row) bool

type CondType string

const (
	GreaterThan CondType = "greater"
	SmallerThan CondType = "smaller"
	Equal       CondType = "equal"
	NotEqual    CondType = "notequal"
	CustomFunc  CondType = "customfunc"
)

type Filter struct {
	Input    Operator
	Cond     FilterFunc
	Column   string
	Value    any
	CondType CondType
}

func (f *Filter) Execute() ([]Row, error) {
	if f.Input == nil {
		return nil, fmt.Errorf("Filter input is nil")
	}

	rows, err := f.Input.Execute()
	if err != nil {
		return nil, err
	}

	var out []Row

	for _, r := range rows {
		if r == nil {
			continue
		}

		if f.evaluateSafe(r) {
			out = append(out, r)
		}
	}

	return out, nil
}

// safe evaluation
func (f *Filter) evaluateSafe(r Row) bool {
	if f.CondType == CustomFunc {
		if f.Cond != nil {
			return f.Cond(r)
		}
		return false
	}

	// check column exists
	val, exists := r[f.Column]
	if !exists || val == nil {
		return false
	}

	switch f.CondType {
	case GreaterThan:
		return toFloat(val) > toFloat(f.Value)
	case SmallerThan:
		return toFloat(val) < toFloat(f.Value)
	case Equal:
		return val == f.Value
	case NotEqual:
		return val != f.Value
	default:
		return false
	}
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case float32:
		return float64(t)
	case float64:
		return t
	}
	return 0
}
