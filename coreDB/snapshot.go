package coreDB

import (
	// "encoding/json"
	// "internal/coverage/rtcov"
	"fmt"
	"os"
	"path/filepath"
)

func newSnapshotID(meta *Metadata) string {
	return fmt.Sprintf("snapshot-%04d", len(meta.Snapshots)+1)
}

func ReadCurrentSnapshot(outputDir string) ([]Record, error) {
	meta, err := LoadMetadata(outputDir)

	if err != nil {
		return nil, err
	}

	return ReadSnapshot(outputDir, meta.CurrentSnapshot)
}

func ReadSnapshot(outputDir string, snapshotID string) ([]Record, error) {
	meta, err := LoadMetadata(outputDir)

	if err != nil {
		return nil, err
	}

	snap := getSnapshot(meta, snapshotID)

	var all []Record

	for _, file := range snap.Files {
		filePath := filepath.Join(outputDir, file.Path)

		recs, err := readSingleFile(filePath, meta.Schema)

		if err != nil {
			return nil, err
		}

		all = append(all, recs...)
	}

	return all, nil

}

func ExpireSnapshots(outputDir string, retainLast int) error {
	meta, err := LoadMetadata(outputDir)
	if err != nil {
		return err
	}

	if len(meta.Snapshots) <= retainLast {
		return nil
	}

	meta.Snapshots = meta.Snapshots[len(meta.Snapshots)-retainLast:]

	meta.CurrentSnapshot = meta.Snapshots[len(meta.Snapshots)-1].ID

	return saveMetadata(outputDir, meta)
}

func GarbageCollect(outputDir string) error {
	meta, err := LoadMetadata(outputDir)
	if err != nil {
		return err
	}

	referenced := make(map[string]bool)

	for _, snap := range meta.Snapshots {
		for _, f := range snap.Files {
			referenced[f.Path] = true
		}
	}

	dataRoot := filepath.Join(outputDir, "data")

	err = filepath.Walk(dataRoot, func(path string, info os.FileInfo, err error) error {
		if filepath.Ext(path) != ".parquet" {
			return nil
		}

		rel, _ := filepath.Rel(outputDir, path)

		if !referenced[rel] {
			os.Remove(path)
		}

		return nil
	})

	return err
}
