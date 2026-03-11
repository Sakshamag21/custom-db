package query

type Limit struct {
	Input Operator
	N     int
}

type VecLimit struct {
	Input VecOperator
	N     int
	read  int
}

func (l *Limit) Execute() ([]Row, error) {
	rows, err := l.Input.Execute()

	if err != nil {
		return nil, err
	}

	if len(rows) <= l.N {
		return rows, nil
	}

	return rows[:l.N], nil
}

func (l *VecLimit) Next() (*Batch, error) {

	if l.read >= l.N {
		return nil, nil
	}

	batch, err := l.Input.Next()

	if err != nil || batch == nil {
		return batch, err
	}

	if l.read+batch.Size > l.N {
		newSize := l.N - l.read

		for _, vec := range batch.Columns {
			vec.Data = vec.Data[:newSize]
		}

		batch.Size = newSize

	}

	l.read += batch.Size

	return batch, nil

}
