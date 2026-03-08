package query

type Limit struct {
	Input Operator
	N     int
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
