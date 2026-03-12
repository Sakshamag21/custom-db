package query

func BuildPhysical(plan LogicalPlan, dbPath string) VecOperator {

	switch p := plan.(type) {

	case *LogicalScan:
		return &VecScan{
			DBPath: dbPath,
		}

	case *LogicalFilter:

		input := BuildPhysical(p.Input, dbPath)

		return &VecFilter{
			Input:    input,
			Column:   p.Cond.Column,
			Value:    p.Cond.Value,
			CondType: p.Cond.Op,
		}

	case *LogicalProjection:

		input := BuildPhysical(p.Input, dbPath)

		return &VecProjection{
			Input:   input,
			Columns: p.Columns,
		}

	case *LogicalLimit:

		input := BuildPhysical(p.Input, dbPath)

		return &VecLimit{
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
