package query

type Vector struct {
	Data []any
}

type Batch struct {
	Columns map[string]*Vector
	Size    int
}
