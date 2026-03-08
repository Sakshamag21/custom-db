package coreDB

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type Record map[string]interface{}

func buildDynamicStruct(schema map[string]string) (interface{}, reflect.Type, error) {

	var fields []reflect.StructField

	// 🔥 SORT KEYS FIRST (CRITICAL)
	var columns []string
	for col := range schema {
		columns = append(columns, col)
	}
	sort.Strings(columns)

	for _, col := range columns {

		typ := schema[col]

		var goType reflect.Type
		var parquetTag string

		switch typ {
		case "STRING":
			goType = reflect.TypeOf("")
			parquetTag = fmt.Sprintf("name=%s, type=BYTE_ARRAY, convertedtype=UTF8", col)

		case "INT64":
			goType = reflect.TypeOf(int64(0))
			parquetTag = fmt.Sprintf("name=%s, type=INT64", col)

		case "INT32":
			goType = reflect.TypeOf(int32(0))
			parquetTag = fmt.Sprintf("name=%s, type=INT32", col)

		default:
			return nil, nil, fmt.Errorf("unsupported type %s", typ)
		}

		fields = append(fields, reflect.StructField{
			Name: strings.Title(col), // Age, Id, Value
			Type: goType,
			Tag:  reflect.StructTag(`parquet:"` + parquetTag + `"`),
		})
	}

	structType := reflect.StructOf(fields)

	return reflect.New(structType).Interface(), structType, nil
}

func mapToStruct(record Record, structType reflect.Type) (interface{}, error) {

	instance := reflect.New(structType).Elem()

	for i := 0; i < structType.NumField(); i++ {

		field := structType.Field(i)
		colName := strings.ToLower(field.Name)

		val, exists := record[colName]
		if !exists {
			continue
		}

		fieldValue := instance.Field(i)

		if !fieldValue.CanSet() {
			continue
		}

		v := reflect.ValueOf(val)

		if v.Type().ConvertibleTo(fieldValue.Type()) {
			fieldValue.Set(v.Convert(fieldValue.Type()))
		} else {
			return nil, fmt.Errorf("type mismatch for column %s", colName)
		}
	}

	return instance.Addr().Interface(), nil
}

func buildSchemaJSON(schema map[string]string) (string, error) {
	type Field struct {
		Tag string
	}

	var fields []Field

	for col, typ := range schema {
		var tag string

		switch typ {
		case "STRING":
			tag = fmt.Sprintf("name =%s, type=BYTE_ARRAY, convertedtype=UTF8", col)
		case "INT64":
			tag = fmt.Sprintf("name=%s, type=INT64", col)
		case "INT32":
			tag = fmt.Sprintf("name=%s, type=INT32", col)
		case "DOUBLE":
			tag = fmt.Sprintf("name=%s, type=DOUBLE", col)
		case "BOOLEAN":
			tag = fmt.Sprintf("name=%s, type=BOOLEAN", col)
		default:
			return "", fmt.Errorf("unsupported type: %s", col)
		}

		fields = append(fields, Field{Tag: tag})
	}

	schemaDef := map[string]interface{}{
		"Tag":    "name=parquet_go_root",
		"Fields": fields,
	}

	jsonBytes, err := json.Marshal(schemaDef)

	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

func writeDynamicFile(filePath string, records []Record, schema map[string]string) error {

	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return err
	}
	defer fw.Close()

	model, structType, err := buildDynamicStruct(schema)
	if err != nil {
		return err
	}

	pw, err := writer.NewParquetWriter(fw, model, 4)
	if err != nil {
		return err
	}
	defer pw.WriteStop()

	for _, record := range records {

		structRecord, err := mapToStruct(record, structType)
		if err != nil {
			return err
		}

		if err := pw.Write(structRecord); err != nil {
			return err
		}
	}

	return nil
}

func getNextPartNumber() (string, error) {
	return uuid.New().String(), nil
}
func getSnapshot(meta Metadata, id string) Snapshot {
	for _, s := range meta.Snapshots {
		if s.ID == id {
			return s
		}
	}

	return Snapshot{}
}

func WriteParquet(records []Record, outputDir string) error {

	if len(records) == 0 {
		return fmt.Errorf("no records provided")
	}

	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return err
	}

	meta, err := loadMetadata(outputDir)
	if err != nil {
		return err
	}

	var newFiles []string

	for _, record := range records {
		for col := range record {
			if _, exists := meta.Schema[col]; !exists {
				return fmt.Errorf("unknown column %s", col)
			}
		}

		for col := range meta.Schema {
			if _, exists := record[col]; !exists {
				return fmt.Errorf("missing required column %s", col)
			}
		}
	}

	grouped := make(map[string][]Record)

	for _, r := range records {
		idVal, ok := r["id"].(string)

		if !ok || idVal == "" {
			return fmt.Errorf("id must be non empty string")
		}
		firstLetter := strings.ToUpper(string(idVal[0]))
		grouped[firstLetter] = append(grouped[firstLetter], r)
	}

	for letter, recs := range grouped {

		partitionDir := filepath.Join(outputDir, "data", letter)

		if err := os.MkdirAll(partitionDir, os.ModePerm); err != nil {
			return err
		}

		nextPart, err := getNextPartNumber()

		if err != nil {
			return err
		}

		fileName := fmt.Sprintf("part-%s.parquet", nextPart)
		filePath := filepath.Join(partitionDir, fileName)

		if err := writeDynamicFile(filePath, recs, meta.Schema); err != nil {
			return err
		}

		relPath := filepath.Join("data", letter, fileName)

		newFiles = append(newFiles, relPath)
	}

	fmt.Println(newFiles)

	const maxRetries = 10

	for retries := 0; retries < maxRetries; retries++ {

		currentMeta, err := loadMetadata(outputDir)
		if err != nil {
			return err
		}

		baseVersion := currentMeta.Version

		// Get current snapshot files
		var allFiles []string
		if currentMeta.CurrentSnapshot != "" {
			prev := getSnapshot(currentMeta, currentMeta.CurrentSnapshot)
			allFiles = append(allFiles, prev.Files...)
		}

		// Add our new files
		allFiles = append(allFiles, newFiles...)

		newSnapshotID := newSnapshotID(&currentMeta)

		snapshot := Snapshot{
			ID:        newSnapshotID,
			Timestamp: time.Now().Format(time.RFC3339),
			Files:     allFiles,
		}

		currentMeta.Snapshots = append(currentMeta.Snapshots, snapshot)
		currentMeta.CurrentSnapshot = newSnapshotID

		err = commitMetadata(outputDir, currentMeta, baseVersion)
		if err != nil {

			if strings.Contains(err.Error(), "concurrent") {
				// Retry only metadata commit
				continue
			}

			return err
		}

		// Success
		return nil
	}

	return fmt.Errorf("commit failed after max retries")
}
