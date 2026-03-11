package query

type LogicalPlan interface{}

type LogicalScan struct {
	Table string
}

type LogicalFilter struct {
	Input LogicalPlan
	Cond  *Condition
}

type LogicalProjection struct {
	Input   LogicalPlan
	Columns []string
}

type LogicalLimit struct {
	Input LogicalPlan
	N     int
}

type LogicalAggregate struct {
	Input   LogicalPlan
	AggType AggType
	Column  string
	GroupBy []string
}
