package query

import "custom_db/coreDB"

type Scan struct {
	DBPath string
}

func (s *Scan) Execute() ([]Row, error) {
	return coreDB.ReadCurrent(s.DBPath)
}
