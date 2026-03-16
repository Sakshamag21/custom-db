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
	op     VecOperator
}

func NewEngine(path string) *Engine {
	return &Engine{
		DBPath: path,
	}
}

func (e *Engine) Reset() {
	e.op = nil
}

func (e *Engine) Next(q Query) (*Batch, error) {

	// build plan only once
	if e.op == nil {

		logical := BuildLogicalPlan(q)

		optimized := Optimize(logical)

		e.op = BuildPhysical(optimized, e.DBPath)
	}

	return e.op.Next()
}
