package query

func BuildLogicalPlan(q Query) LogicalPlan {
	var plan LogicalPlan
	plan = &LogicalScan{
		Table: "benchdb",
	}

	if q.Where != nil {
		plan = &LogicalFilter{
			Input: plan,
			Cond:  q.Where,
		}
	}

	if q.Agg != nil {
		plan = &LogicalAggregate{
			Input:   plan,
			AggType: q.Agg.Type,
			Column:  q.Agg.Column,
			GroupBy: q.Agg.GroupBy,
		}
	}

	if len(q.Select) > 0 {
		plan = &LogicalProjection{
			Input:   plan,
			Columns: q.Select,
		}
	}

	if q.Limit > 0 {
		plan = &LogicalLimit{
			Input: plan,
			N:     q.Limit,
		}
	}

	return plan
}
