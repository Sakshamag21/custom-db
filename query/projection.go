package query

type Projection struct {
	Input   Operator
	Columns []string
}

type VecProjection struct {
	Input   VecOperator
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

func (p *VecProjection) Next() (*Batch, error) {

	batch, err := p.Input.Next()

	if err != nil || batch == nil {
		return batch, err
	}

	out := &Batch{
		Columns: make(map[string]*Vector),
		Size:    batch.Size,
	}

	for _, col := range p.Columns {
		out.Columns[col] = batch.Columns[col]
	}

	return out, nil
}
