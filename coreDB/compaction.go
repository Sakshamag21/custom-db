package coreDB

import (
	// "encoding/json"
	// "internal/coverage/rtcov"
	// "os"
	"fmt"
	"path/filepath"
	"time"
)

func CompactCurrentSnapshot(outputDir string) error {

	meta, err := loadMetadata(outputDir)
	if err != nil {
		return err
	}

	current := getSnapshot(meta, meta.CurrentSnapshot)

	if len(current.Files) == 0 {
		return fmt.Errorf("current snapshot has no files")
	}

	// Group files by partition
	partitions := make(map[string][]string)

	for _, f := range current.Files {
		fullPath := filepath.Join(outputDir, f)
		dir := filepath.Dir(fullPath)
		partitions[dir] = append(partitions[dir], fullPath)
	}

	var newFiles []string

	for partitionDir, files := range partitions {

		if len(files) == 1 {
			// keep file as is
			rel, _ := filepath.Rel(outputDir, files[0])
			newFiles = append(newFiles, rel)
			continue
		}

		var all []Record

		for _, file := range files {
			recs, err := readSingleFile(file, meta.Schema)
			if err != nil {
				return err
			}
			all = append(all, recs...)
		}

		nextPart, err := getNextPartNumber()
		if err != nil {
			return err
		}

		fileName := fmt.Sprintf("compact-%s.parquet", nextPart)
		newPath := filepath.Join(partitionDir, fileName)

		if err := writeDynamicFile(newPath, all, meta.Schema); err != nil {
			return err
		}

		rel, _ := filepath.Rel(outputDir, newPath)
		newFiles = append(newFiles, rel)
	}

	// 🚨 CRITICAL GUARD
	if len(newFiles) == 0 {
		return fmt.Errorf("compaction produced no files")
	}

	newID := newSnapshotID(&meta)

	snap := Snapshot{
		ID:        newID,
		Timestamp: time.Now().Format(time.RFC3339),
		Files:     newFiles,
	}

	meta.Snapshots = append(meta.Snapshots, snap)
	meta.CurrentSnapshot = newID

	return saveMetadata(outputDir, meta)
}
