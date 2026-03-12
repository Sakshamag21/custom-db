// package main

// import (
// 	"custom_db/coreDB"
// 	"fmt"
// 	"log"
// )

// func main() {

// 	dbPath := "./output"

// 	// schema := map[string]string{
// 	// 	"id":    "STRING",
// 	// 	"value": "INT64",
// 	// 	"age":   "INT32",
// 	// }

// 	// err := coreDB.CreateDB(dbPath, schema)
// 	// if err != nil {
// 	// 	fmt.Println("CreateDB:", err)
// 	// }

// 	// batch1 := []coreDB.Record{
// 	// 	{"id": "apple123", "value": int64(10), "age": int32(25)},
// 	// 	{"id": "banana456", "value": int64(20), "age": int32(30)},
// 	// }

// 	// err = coreDB.WriteParquet(batch1, dbPath)
// 	// if err != nil {
// 	// 	log.Fatal("Write Error:", err)
// 	// }

// 	// fmt.Println("Batch 1 inserted.")

// 	// batch2 := []coreDB.Record{
// 	// 	{"id": "apricot789", "value": int64(40), "age": int32(35)},
// 	// 	{"id": "blueberry111", "value": int64(50), "age": int32(28)},
// 	// }

// 	// err = coreDB.WriteParquet(batch2, dbPath)
// 	// if err != nil {
// 	// 	log.Fatal("Write Error:", err)
// 	// }

// 	// fmt.Println("Batch 2 inserted.")

// 	// currentRecords, err := coreDB.ReadCurrent(dbPath)
// 	// if err != nil {
// 	// 	log.Fatal("ReadCurrent Error:", err)
// 	// }

// 	// fmt.Println("\nCurrent Snapshot Records:", len(currentRecords))
// 	// for _, r := range currentRecords {
// 	// 	fmt.Println(r)
// 	// }

// 	oldSnapshotID := "snapshot-0001"
// 	fmt.Println(oldSnapshotID)

// 	oldRecords, err := coreDB.ReadSnapshot(dbPath, oldSnapshotID)
// 	if err != nil {
// 		log.Fatal("ReadSnapshot Error:", err)
// 	}

// 	fmt.Println("\nSnapshot:", oldSnapshotID)
// 	fmt.Println("Records:", len(oldRecords))
// 	for _, r := range oldRecords {
// 		fmt.Println(r)
// 	}

// 	oldSnapshotID = "snapshot-0004"
// 	fmt.Println(oldSnapshotID)

// 	oldRecords, err = coreDB.ReadSnapshot(dbPath, oldSnapshotID)
// 	if err != nil {
// 		log.Fatal("ReadSnapshot Error:", err)
// 	}

// 	fmt.Println("\nSnapshot:", oldSnapshotID)
// 	fmt.Println("Records:", len(oldRecords))
// 	for _, r := range oldRecords {
// 		fmt.Println(r)
// 	}

// 	err = coreDB.CompactCurrentSnapshot(dbPath)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	fmt.Println("Compaction completed")

// 	// oldSnapshotID = "snapshot-0004"
// 	// fmt.Println(oldSnapshotID)

// 	// oldRecords, err = coreDB.ReadSnapshot(dbPath, oldSnapshotID)
// 	// if err != nil {
// 	// 	log.Fatal("ReadSnapshot Error:", err)
// 	// }

// 	// fmt.Println("\nSnapshot:", oldSnapshotID)
// 	// fmt.Println("Records:", len(oldRecords))
// 	// for _, r := range oldRecords {
// 	// 	fmt.Println(r)
// 	// }

// 	// err = coreDB.ExpireSnapshots(dbPath, 1)
// 	// if err != nil {
// 	// 	log.Fatal("ExpireSnapshots Error:", err)
// 	// }

// 	// fmt.Println("\nExpired old snapshots. Keeping only latest.")

// 	// err = coreDB.GarbageCollect(dbPath)
// 	// if err != nil {
// 	// 	log.Fatal("GarbageCollect Error:", err)
// 	// }

// 	// fmt.Println("Garbage collection completed.")
// }

package main

import (
	"fmt"
	// "log"
	// "sync"
	// "time"

	// "custom_db/coreDB"
	"custom_db/query"
)

func main() {

	// outputDir := "./mydb"

	// schema := map[string]string{
	// 	"id":    "STRING",
	// 	"age":   "INT32",
	// 	"value": "INT64",
	// }

	// err := coreDB.CreateDB(outputDir, schema)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// var wg sync.WaitGroup

	// writer := func(writerID int) {
	// 	defer wg.Done()

	// 	records := []coreDB.Record{
	// 		{
	// 			"id":    fmt.Sprintf("a_user_%d_%d", writerID, time.Now().UnixNano()),
	// 			"age":   20 + writerID,
	// 			"value": int64(1000 + writerID),
	// 		},
	// 		{
	// 			"id":    fmt.Sprintf("b_user_%d_%d", writerID, time.Now().UnixNano()),
	// 			"age":   30 + writerID,
	// 			"value": int64(2000 + writerID),
	// 		},
	// 	}

	// 	err := coreDB.WriteParquet(records, outputDir)
	// 	if err != nil {
	// 		fmt.Println("Writer", writerID, "failed:", err)
	// 		return
	// 	}

	// 	fmt.Println("Writer", writerID, "committed successfully")
	// }

	// // Launch 5 concurrent writers
	// for i := 0; i < 5; i++ {
	// 	wg.Add(1)
	// 	go writer(i)
	// }

	// wg.Wait()

	// results, err := coreDB.ReadCurrent(outputDir)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println("\nFinal record count:", len(results))

	engine := query.NewEngine("mydb")

	q := query.Query{
		Select: []string{"id", "age"},
		Where: &query.Condition{
			Column: "age",
			Op:     query.GreaterThan,
			Value:  30,
		},
		Limit: 5,
	}

	for {
		batch, err := engine.Next(q)
		if err != nil {
			panic(err)
		}

		if batch == nil {
			break
		}

		fmt.Println(batch)

		for i := 0; i < batch.Size; i++ {
			row := make(map[string]any)

			for col, vec := range batch.Columns {
				row[col] = vec.Data[i]
			}

			fmt.Println(row)
		}
	}
}
