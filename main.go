package main

import (
	"fmt"
	"math/rand"
	"time"

	"custom_db/coreDB"
	"custom_db/query"
)

const (
	DBPath = "./benchdb"
	Rows   = 1_000_000
)

func main() {

	fmt.Println("==== Custom Parquet DB Benchmark ====")
	fmt.Println("Rows:", Rows)

	//-------------------------------------
	// Schema
	//-------------------------------------

	schema := map[string]string{
		"id":    "STRING",
		"age":   "INT32",
		"value": "INT64",
	}

	err := coreDB.CreateDB(DBPath, schema)
	if err != nil {
		fmt.Println("DB may already exist")
	}

	//-------------------------------------
	// Bloom Config
	//-------------------------------------

	bloomConfig := coreDB.BloomConfig{
		Columns: []string{"id"},
		Size:    10000,
		Hashes:  4,
	}

	//-------------------------------------
	// Generate dataset (partitioned A-Z)
	//-------------------------------------

	fmt.Println("\nGenerating dataset...")

	records := make([]coreDB.Record, Rows)

	for i := 0; i < Rows; i++ {

		letter := 'A' + rune(rand.Intn(26))

		id := fmt.Sprintf("%c_user_%d", letter, i)

		records[i] = coreDB.Record{
			"id":    id,
			"age":   rand.Intn(100),
			"value": rand.Int63n(100000),
		}
	}

	//-------------------------------------
	// Write Benchmark
	//-------------------------------------

	fmt.Println("\nWriting data...")

	start := time.Now()

	err = coreDB.WriteParquet(records, DBPath, bloomConfig)
	if err != nil {
		panic(err)
	}

	writeTime := time.Since(start)

	fmt.Println("Write time:", writeTime)
	fmt.Println("Write throughput:", Rows/int(writeTime.Seconds()), "rows/sec")

	//-------------------------------------
	// Full Scan Benchmark
	//-------------------------------------

	engine := query.NewEngine(DBPath)

	fullQuery := query.Query{
		Select: []string{"id", "age", "value"},
	}

	fmt.Println("\nRunning full scan...")

	start = time.Now()

	total := 0

	for {
		batch, err := engine.Next(fullQuery)
		if err != nil {
			panic(err)
		}

		if batch == nil {
			break
		}

		total += batch.Size
	}

	scanTime := time.Since(start)

	fmt.Println("Rows scanned:", total)
	throughput := float64(total) / scanTime.Seconds()
	fmt.Printf("Scan time: %v\n", scanTime)
	fmt.Printf("Scan throughput: %.2f rows/sec\n", throughput)

	//-------------------------------------
	// Filter Benchmark
	//-------------------------------------

	filterQuery := query.Query{
		Select: []string{"id", "age"},
		Where: &query.Condition{
			Column: "age",
			Op:     query.GreaterThan,
			Value:  50,
		},
	}

	fmt.Println("\nRunning filter query...")

	start = time.Now()

	total = 0

	engine.Reset()

	for {
		batch, err := engine.Next(filterQuery)
		if err != nil {
			panic(err)
		}

		if batch == nil {
			break
		}

		total += batch.Size
	}

	filterTime := time.Since(start)

	fmt.Println("Rows returned:", total)
	fmt.Println("Filter time:", filterTime)

	//-------------------------------------
	// Bloom Filter Benchmark
	//-------------------------------------

	eqQuery := query.Query{
		Select: []string{"id", "age"},
		Where: &query.Condition{
			Column: "id",
			Op:     query.Equal,
			Value:  "A_user_100",
		},
	}

	fmt.Println("\nRunning equality query (Bloom test)...")

	start = time.Now()

	total = 0

	engine.Reset()

	for {
		batch, err := engine.Next(eqQuery)
		if err != nil {
			panic(err)
		}

		if batch == nil {
			break
		}

		total += batch.Size
	}

	bloomTime := time.Since(start)

	fmt.Println("Rows returned:", total)
	fmt.Println("Equality query time:", bloomTime)

	fmt.Println("\n==== Benchmark Complete ====")
}
