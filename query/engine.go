package query

import (
	"custom_db/coreDB"
)

type Row = coreDB.Record

type Operator interface {
	Execute() ([]Row, error)
}

type VecOperator interface {
	Next() (*Batch, error)
}

type Engine struct {
	DBPath string
}

func NewEngine(path string) *Engine {
	return &Engine{
		DBPath: path,
	}
}

func (e *Engine) Execute(q Query) ([]Row, error) {
	// var op Operator

	// op = &Scan{
	// 	DBPath: e.DBPath,
	// }

	// if q.Where != nil {
	// 	op = &Filter{
	// 		Input:    op,
	// 		Column:   q.Where.Column,
	// 		Value:    q.Where.Value,
	// 		CondType: q.Where.Op,
	// 	}
	// }

	// if len(q.Select) > 0 {
	// 	op = &Projection{
	// 		Input:   op,
	// 		Columns: q.Select,
	// 	}
	// }

	// if q.Agg != nil {
	// 	op = &Aggregate{
	// 		Input:   op,
	// 		GroupBy: q.GroupBy,
	// 		Type:    q.Agg.Type,
	// 		Column:  q.Agg.Column,
	// 	}
	// }

	// if q.Limit > 0 {
	// 	op = &Limit{
	// 		Input: op,
	// 		N:     q.Limit,
	// 	}
	// }

	logical := BuildLogicalPlan(q)

	optimized := Optimize(logical)

	physical := BuildPhysical(optimized, e.DBPath)

	return physical.Execute()
}
