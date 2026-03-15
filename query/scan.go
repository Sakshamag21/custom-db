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
	DBPath      string
	BloomColumn string
	BloomValue  string
	data        []Row
	pos         int
	loaded      bool
}

const BatchSize = 1024

func (s *VecScan) Next() (*Batch, error) {

	// 🔹 Load data only once
	if !s.loaded {

		var rows []Row
		var err error

		if s.BloomColumn != "" {
			meta, err := coreDB.LoadMetadata(s.DBPath)

			if err != nil {
				return nil, err
			}

			files, err := coreDB.BloomPruneFiles(
				s.DBPath,
				s.BloomColumn,
				s.BloomValue,
			)

			if err != nil {
				return nil, err
			}

			rows, err = coreDB.ReadSelectedFiles(files, meta.Schema)

			if err != nil {
				return nil, err
			}
		} else {
			rows, err = coreDB.ReadCurrent(s.DBPath)
			if err != nil {
				return nil, err
			}
		}

		s.data = rows
		s.loaded = true
	}

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

	// create column vectors
	for col := range rows[0] {
		batch.Columns[col] = &Vector{
			Data: make([]any, len(rows)),
		}
	}

	// fill vectors
	for i, r := range rows {
		for col, val := range r {
			batch.Columns[col].Data[i] = val
		}
	}

	return batch, nil
}
