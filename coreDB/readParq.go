package coreDB

import (
	// "os"
	// "fmt"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func GetAllFiles(outputDir string) ([]string, error) {

	meta, err := LoadMetadata(outputDir)
	if err != nil {
		return nil, err
	}

	snap := getSnapshot(meta, meta.CurrentSnapshot)

	var files []string

	for _, f := range snap.Files {
		full := filepath.Join(outputDir, f.Path)
		files = append(files, full)
	}

	return files, nil
}

func readSingleFile(filePath string, schema map[string]string) ([]Record, error) {

	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return nil, err
	}
	defer fr.Close()

	model, structType, err := buildDynamicStruct(schema)
	if err != nil {
		return nil, err
	}

	pr, err := reader.NewParquetReader(fr, model, 4)
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())
	if num == 0 {
		return []Record{}, nil
	}

	sliceType := reflect.SliceOf(structType)
	sliceVal := reflect.MakeSlice(sliceType, num, num)
	slicePtr := reflect.New(sliceType)
	slicePtr.Elem().Set(sliceVal)

	if err := pr.Read(slicePtr.Interface()); err != nil {
		return nil, err
	}

	var results []Record

	for i := 0; i < num; i++ {
		structVal := slicePtr.Elem().Index(i)
		record := make(Record)

		for j := 0; j < structType.NumField(); j++ {
			field := structType.Field(j)
			colName := strings.ToLower(field.Name)
			record[colName] = structVal.Field(j).Interface()
		}

		results = append(results, record)
	}

	return results, nil
}

func ReadCurrent(outputDir string) ([]Record, error) {

	meta, err := LoadMetadata(outputDir)
	if err != nil {
		return nil, err
	}

	return ReadSnapshot(outputDir, meta.CurrentSnapshot)
}
