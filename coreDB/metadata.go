package coreDB

import (
	"encoding/json"
	"fmt"
	// "internal/coverage/rtcov"
	"os"
	"path/filepath"
)

type Snapshot struct {
	ID        string
	Timestamp string
	Files     []string
}

type Metadata struct {
	Version         int
	Schema          map[string]string
	CurrentSnapshot string
	Snapshots       []Snapshot
}

func metadataPath(outputDir string) string {
	return filepath.Join(outputDir, "metadata.json")
}

func loadMetadata(outputDir string) (Metadata, error) {

	var meta Metadata

	data, err := os.ReadFile(metadataPath(outputDir))

	if err != nil {
		return meta, err
	}

	err = json.Unmarshal(data, &meta)

	return meta, err
}

func saveMetadata(outputDir string, meta Metadata) error {

	data, err := json.MarshalIndent(meta, "", " ")

	if err != nil {
		return err
	}

	metaPath := metadataPath(outputDir)
	tmpPath := metaPath + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)

	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, metaPath)
}

func commitMetadata(outputDir string, newMeta Metadata, expectedVersion int) error {
	currentMeta, err := loadMetadata(outputDir)

	if err != nil {
		return err
	}

	if currentMeta.Version != expectedVersion {
		return fmt.Errorf("concurrent modification detected")
	}

	newMeta.Version = expectedVersion + 1

	return saveMetadata(outputDir, newMeta)
}
