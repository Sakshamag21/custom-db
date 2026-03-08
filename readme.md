# Custom Parquet DB

A lightweight, single-node Parquet-based database with snapshotting, atomic metadata management, OCC-based writes, and a SQL-like query engine in Go.

---

## Features

### Storage & Metadata

* Stores data in **Parquet files** partitioned by the first letter of the `id`.
* Maintains **snapshots** for atomic commits and rollback.
* Uses **atomic metadata writes** to avoid corruption.
* Supports **Optimistic Concurrency Control (OCC)** for safe parallel writes.

### Schema Management

* `CreateDB(outputDir, schema)` — create a database with a schema.
* `AddColumn(baseDir, column, type)` — add a new column.
* `DropColumn(baseDir, column)` — remove a column.

### Data Writing

* `WriteParquet(records, outputDir)` — writes data to Parquet, creates snapshots.
* Supports **retry logic** with OCC for parallel writers.
* Automatically generates **part files** and groups them by `id`.

### Compaction & Garbage Collection

* `GarbageCollect(outputDir)` — removes orphaned Parquet files.
* Can compact old snapshots safely without affecting the active snapshot.

### Query Engine

Supports SQL-like operations using operators:

* **Scan** — read all rows from DB.
* **Filter** — filter rows by condition (`>`, `<`, `=`, `!=`, or custom function).
* **Projection** — select specific columns.
* **Limit** — limit the number of returned rows.
* **Aggregate** — supports `COUNT`, `SUM`, `AVG` with optional `GROUP BY`.

Example:

```go
q := query.Query{
    Select: []string{"id", "age"},
    Where: &query.Condition{
        Column: "age",
        Op:     query.GreaterThan,
        Value:  30,
    },
    Limit: 5,
}
rows, _ := engine.Execute(q)
```

Equivalent SQL:

```sql
SELECT id, age
FROM mydb
WHERE age > 30
LIMIT 5;
```

---

## Installation

```bash
git clone <repo-url>
cd custom_db
go mod tidy
```

---

## Usage

### Creating a DB

```go
schema := map[string]string{
    "id": "STRING",
    "age": "INT32",
    "value": "INT64",
}

err := coreDB.CreateDB("mydb", schema)
```

### Writing Data

```go
records := []coreDB.Record{
    {"id": "A1", "age": 25, "value": 100},
    {"id": "B2", "age": 30, "value": 200},
}

err := coreDB.WriteParquet(records, "mydb")
```

### Querying Data

```go
engine := query.NewEngine("mydb")

q := query.Query{
    Select: []string{"id", "value"},
    Where: &query.Condition{
        Column: "age",
        Op:     query.GreaterThan,
        Value:  20,
    },
    Limit: 10,
}

rows, _ := engine.Execute(q)
for _, r := range rows {
    fmt.Println(r)
}
```

### Aggregation with Group By

```go
agg := &query.Aggregate{
    Input:   scan,
    Column:  "value",
    Type:    query.SUM,
    GroupBy: []string{"age"},
}

rows, _ := agg.Execute()
```

---

## Architecture

```
Parquet Files (partitioned by id)
        ↓
       Scan
        ↓
      Filter
        ↓
    Projection
        ↓
     Aggregate
        ↓
      Limit
        ↓
      Results
```

* **Operators** form a pipeline.
* **Snapshots** ensure atomic commits.
* **OCC** prevents conflicts during concurrent writes.
* **Compaction** and **GC** maintain storage efficiency.

---

## Future Improvements

* Full SQL parser to allow queries as strings.
* Streaming row iterators for memory-efficient query execution.
* Predicate pushdown to scan only relevant files.
* Parallel file scanning for faster query performance.

---

## License

MIT License

---
