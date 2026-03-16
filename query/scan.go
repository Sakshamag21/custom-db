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

	RGColumn string
	RGValue  float64
	data     []Row
	pos      int
	loaded   bool
}

const BatchSize = 1024

func (e *Engine) buildPlan(q Query) {

	scan := &VecScan{
		DBPath: e.DBPath,
	}

	var op VecOperator = scan

	if q.Where != nil {
		op = &VecFilter{
			Input:    op,
			Column:   q.Where.Column,
			Value:    q.Where.Value,
			CondType: q.Where.Op,
		}
	}

	e.op = op
}

func (s *VecScan) Next() (*Batch, error) {

	if !s.loaded {

		var rows []Row
		var err error

		meta, err := coreDB.LoadMetadata(s.DBPath)
		if err != nil {
			return nil, err
		}

		var files []string

		// 🔹 Bloom pruning
		if s.BloomColumn != "" {

			files, err = coreDB.BloomPruneFiles(
				s.DBPath,
				s.BloomColumn,
				s.BloomValue,
			)

			if err != nil {
				return nil, err
			}

		} else {

			files, err = coreDB.GetAllFiles(s.DBPath)
			if err != nil {
				return nil, err
			}
		}

		// 🔹 Row-group pruning
		if s.RGColumn != "" {

			rows, err = coreDB.ReadFilesWithRowGroupPruning(
				files,
				meta.Schema,
				s.RGColumn,
				s.RGValue,
			)

			if err != nil {
				return nil, err
			}

		} else {

			rows, err = coreDB.ReadSelectedFiles(files, meta.Schema)

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
