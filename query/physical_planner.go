package query

func BuildPhysical(plan LogicalPlan, dbPath string) Operator {

	switch p := plan.(type) {

	case *LogicalScan:
		return &Scan{
			DBPath: dbPath,
		}

	case *LogicalFilter:

		input := BuildPhysical(p.Input, dbPath)

		return &Filter{
			Input:    input,
			Column:   p.Cond.Column,
			Value:    p.Cond.Value,
			CondType: p.Cond.Op,
		}

	case *LogicalProjection:

		input := BuildPhysical(p.Input, dbPath)

		return &Projection{
			Input:   input,
			Columns: p.Columns,
		}

	case *LogicalLimit:

		input := BuildPhysical(p.Input, dbPath)

		return &Limit{
			Input: input,
			N:     p.N,
		}

	case *LogicalAggregate:

		input := BuildPhysical(p.Input, dbPath)

		return &Aggregate{
			Input:   input,
			Column:  p.Column,
			Type:    p.AggType,
			GroupBy: p.GroupBy,
		}

	}

	return nil
}
