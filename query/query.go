package query

type Condition struct {
	Column string
	Op     CondType
	Value  any
}

type Query struct {
	Select  []string
	Where   *Condition
	Limit   int
	GroupBy []string
	Agg     *Aggregate
}
