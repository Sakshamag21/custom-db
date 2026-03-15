package coreDB

import (
	"fmt"
	"os"
)

func CreateDB(outputDir string, schema map[string]string) error {

	if _, err := os.Stat(outputDir); err == nil {
		return fmt.Errorf("database already exsist")
	}

	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return err
	}

	meta := Metadata{
		Version:         1,
		CurrentSnapshot: "",
		Schema:          schema,
		Snapshots:       []Snapshot{},
	}

	return saveMetadata(outputDir, meta)
}

func AddColumn(baseDir string, column string, colType string) error {
	meta, err := LoadMetadata(baseDir)
	if err != nil {
		return err
	}

	if _, exists := meta.Schema[column]; exists {
		return fmt.Errorf("column already exsists")
	}

	meta.Schema[column] = colType

	return saveMetadata(baseDir, meta)
}

func DropColumn(baseDir string, column string) error {
	meta, err := LoadMetadata(baseDir)

	if err != nil {
		return err
	}

	if _, exists := meta.Schema[column]; !exists {
		return fmt.Errorf("column does not exist")
	}

	delete(meta.Schema, column)

	return saveMetadata(baseDir, meta)
}
