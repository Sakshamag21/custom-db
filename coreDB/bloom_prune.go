package coreDB

func BloomPruneFiles(outputDir string, column string, value string) ([]string, error) {

	meta, err := LoadMetadata(outputDir)

	if err != nil {
		return nil, err
	}

	snap := getSnapshot(meta, meta.CurrentSnapshot)

	var selected []string

	for _, f := range snap.Files {

		fullPath := outputDir + "/" + f.Path

		bloom, err := loadBloomFile(fullPath)

		if err != nil {
			selected = append(selected, fullPath)
			continue
		}

		filter, ok := bloom.Columns[column]

		if !ok {
			selected = append(selected, fullPath)
			continue
		}

		if filter.MightContain(value) {
			selected = append(selected, fullPath)
		}
	}

	return selected, nil
}

func ReadSelectedFiles(files []string, schema map[string]string) ([]Record, error) {

	var all []Record

	for _, f := range files {

		recs, err := readSingleFile(f, schema)
		if err != nil {
			return nil, err
		}

		all = append(all, recs...)
	}

	return all, nil
}
