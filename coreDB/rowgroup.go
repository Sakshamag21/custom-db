package coreDB

import (
	"encoding/json"
	"os"
)

type RowGroupStat struct {
	Min interface{} `json:"min"`
	Max interface{} `json:"max"`
}

type RowGroupMeta struct {
	Index   int                     `json:"index"`
	Columns map[string]RowGroupStat `json:"columns"`
}

type FileRowGroups struct {
	File      string         `json:"file"`
	RowGroups []RowGroupMeta `json:"rowgroups"`
}

func buildRowGroups(records []Record, schema map[string]string, groupSize int) []RowGroupMeta {

	var groups []RowGroupMeta

	for start := 0; start < len(records); start += groupSize {

		end := start + groupSize
		if end > len(records) {
			end = len(records)
		}

		rows := records[start:end]

		stats := computeZoneMap(rows, schema)

		groups = append(groups, RowGroupMeta{
			Index:   len(groups),
			Columns: convertZoneMap(stats),
		})
	}

	return groups
}

func convertZoneMap(z map[string]ZoneMap) map[string]RowGroupStat {

	out := make(map[string]RowGroupStat)

	for col, v := range z {
		out[col] = RowGroupStat{
			Min: v.Min,
			Max: v.Max,
		}
	}

	return out
}

func saveRowGroupMeta(filePath string, groups []RowGroupMeta) error {

	meta := FileRowGroups{
		File:      filePath,
		RowGroups: groups,
	}

	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	return os.WriteFile(filePath+".rg", data, 0644)
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case int:
		return float64(t)
	case int32:
		return float64(t)
	case int64:
		return float64(t)
	case float32:
		return float64(t)
	case float64:
		return t
	}
	return 0
}

func pruneRowGroups(file string, column string, value float64) ([]int, error) {

	var meta FileRowGroups

	data, err := os.ReadFile(file + ".rg")
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &meta)
	if err != nil {
		return nil, err
	}

	var selected []int

	for _, rg := range meta.RowGroups {

		stat := rg.Columns[column]

		if value <= toFloat(stat.Max) {
			selected = append(selected, rg.Index)
		}
	}

	return selected, nil
}

func ShouldReadFileByRowGroup(file string, column string, value float64) bool {

	data, err := os.ReadFile(file + ".rg")

	if err != nil {
		// no rowgroup metadata → must read
		return true
	}

	var meta FileRowGroups

	err = json.Unmarshal(data, &meta)
	if err != nil {
		return true
	}

	for _, rg := range meta.RowGroups {

		stat := rg.Columns[column]

		max := toFloat(stat.Max)

		if value <= max {
			return true
		}
	}

	return false
}

func ReadFilesWithRowGroupPruning(
	files []string,
	schema map[string]string,
	column string,
	value float64,
) ([]Record, error) {

	var all []Record

	for _, f := range files {

		if column != "" {

			ok := ShouldReadFileByRowGroup(f, column, value)

			if !ok {
				continue
			}
		}

		recs, err := readSingleFile(f, schema)

		if err != nil {
			return nil, err
		}

		all = append(all, recs...)
	}

	return all, nil
}
