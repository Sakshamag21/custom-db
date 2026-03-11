package query

import (
	"custom_db/coreDB"
	// "internal/coverage/rtcov"
)

type Scan struct {
	DBPath string
}

func (s *Scan) Execute() ([]Row, error) {
	return coreDB.ReadCurrent(s.DBPath)
}

type VecScan struct {
	DBPath string
	data   []Row
	pos    int
}

const BatchSize = 1024

func (s *VecScan) Next() (*Batch, error) {

	if s.pos >= len(s.data) {
		return nil, nil
	}

	end := s.pos + BatchSize
	if end > len(s.data) {
		end = len(s.data)
	}

	rows := s.data[s.pos:end]
	s.pos = end

	batch := &Batch{
		Columns: make(map[string]*Vector),
		Size:    len(rows),
	}

	for col := range rows[0] {
		batch.Columns[col] = &Vector{
			Data: make([]any, len(rows)),
		}
	}

	for i, r := range rows {
		for col, val := range r {
			batch.Columns[col].Data[i] = val
		}
	}

	return batch, nil

}
