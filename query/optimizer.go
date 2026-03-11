package query

func Optimize(plan LogicalPlan) LogicalPlan {
	switch p := plan.(type) {
	case *LogicalProjection:
		if filter, ok := p.Input.(*LogicalFilter); ok {
			return &LogicalFilter{
				Input: &LogicalProjection{
					Input:   filter.Input,
					Columns: p.Columns,
				},

				Cond: filter.Cond,
			}
		}
	}

	return plan
}
