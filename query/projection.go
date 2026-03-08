package query

type Projection struct {
	Input   Operator
	Columns []string
}

func (p *Projection) Execute() ([]Row, error) {
	rows, err := p.Input.Execute()
	if err != nil {
		return nil, err
	}

	var result []Row

	for _, r := range rows {
		row := make(Row)

		for _, col := range p.Columns {
			row[col] = r[col]
		}

		result = append(result, row)
	}

	return result, nil
}
