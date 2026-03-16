# Custom Parquet DB

A lightweight **single-node analytical database** built on **Parquet** with snapshot-based storage, atomic metadata commits, and a vectorized query engine written in Go.

The system is inspired by modern data lake and analytical engines like DuckDB, ClickHouse, and Apache Iceberg.

---

# Features

## Storage Engine

* Data stored in **Parquet files** for efficient columnar analytics.
* Files are **partitioned by the first letter of `id`**.
* Each write produces a **snapshot**, enabling atomic commits.
* **Metadata is written atomically** to avoid corruption.
* Uses **Optimistic Concurrency Control (OCC)** for safe concurrent writes.

---

## Schema Management

Supports schema evolution:

* `CreateDB(outputDir, schema)` — initialize a database.
* `AddColumn(baseDir, column, type)` — add a column.
* `DropColumn(baseDir, column)` — remove a column.

Schema types supported:

```
STRING
INT32
INT64
DOUBLE
BOOLEAN
```

---

## Data Writing

`WriteParquet(records, outputDir)`:

* Writes records to partitioned Parquet files.
* Creates a **new snapshot** for each successful commit.
* Uses **retry logic with OCC** for concurrent writers.
* Automatically generates **part files**.

Example:

```go
records := []coreDB.Record{
    {"id": "A1", "age": 25, "value": 100},
    {"id": "B2", "age": 30, "value": 200},
}

err := coreDB.WriteParquet(records, "mydb")
```

---

## Snapshot System

Snapshots provide:

* Atomic commits
* Time-travel potential
* Safe compaction
* Concurrent writer safety

Metadata example:

```
metadata.json
 ├ version
 ├ schema
 ├ currentSnapshot
 └ snapshots
```

---

## Query Engine

The query engine uses **operator pipelines** similar to modern analytical databases.

Operators include:

* **Scan** – reads Parquet files
* **Filter** – apply conditions (`>`, `<`, `=`, `!=`)
* **Projection** – select specific columns
* **Aggregate** – `COUNT`, `SUM`, `AVG`
* **Limit** – restrict result size

Example query:

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

## Vectorized Execution

The engine supports **vectorized processing** using column vectors and batches.

Execution flow:

```
Parquet Files
     ↓
VecScan
     ↓
VecFilter
     ↓
Projection
     ↓
Aggregation
     ↓
Limit
```

Vectorized execution significantly improves analytical query performance.

---

## Query Pruning Optimizations

To avoid scanning unnecessary data, the engine supports multiple pruning techniques.

### Partition Pruning

Files are partitioned by `id` prefix:

```
data/
 ├ A/
 ├ B/
 ├ C/
```

Only relevant partitions are scanned.

---

### Bloom Filters

User-defined bloom filters allow fast equality pruning.

Example configuration:

```
BloomFilter columns: ["id", "email"]
```

Before scanning a file:

```
bloom check → skip file if value impossible
```

---

### Zone Maps

Each file stores column statistics:

```
age: min=18 max=65
value: min=10 max=1000
```

Queries can skip files where conditions cannot match.

Example:

```
WHERE age > 80
```

File skipped if:

```
max(age) < 80
```

---

### Row Group Pruning

Parquet files are divided into **row groups**.

Example:

```
file.parquet
 ├ rowgroup1 (age 0–20)
 ├ rowgroup2 (age 21–40)
 └ rowgroup3 (age 41–80)
```

Query:

```
WHERE age > 50
```

Only rowgroup3 must be scanned.

---

## Compaction

Small files can be merged into larger ones.

```
CompactCurrentSnapshot(outputDir)
```

Benefits:

* fewer files
* faster scans
* improved storage efficiency

---

## Garbage Collection

Orphaned files can be safely removed:

```
GarbageCollect(outputDir)
```

Ensures unused Parquet files from older snapshots are deleted.

---

# Installation

```
git clone <repo-url>
cd custom_db
go mod tidy
```

---

# Usage

## Creating a Database

```go
schema := map[string]string{
    "id": "STRING",
    "age": "INT32",
    "value": "INT64",
}

err := coreDB.CreateDB("mydb", schema)
```

---

## Writing Data

```go
records := []coreDB.Record{
    {"id": "A1", "age": 25, "value": 100},
    {"id": "B2", "age": 30, "value": 200},
}

err := coreDB.WriteParquet(records, "mydb")
```

---

## Querying Data

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

---

## Aggregation Example

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

# Architecture

```
             Query
               ↓
           Planner
               ↓
        Operator Pipeline
               ↓
        ┌───────────────┐
        │ VecScan       │
        └──────┬────────┘
               ↓
        Bloom Filter
               ↓
        Zone Map Pruning
               ↓
        Row Group Pruning
               ↓
           VecFilter
               ↓
           Projection
               ↓
           Aggregate
               ↓
             Limit
               ↓
            Result
```

---

# Future Improvements

Planned enhancements:

* SQL parser for string queries
* Streaming parquet reader (avoid loading full dataset)
* Parallel file scanning
* Cost-based query planner
* Index structures
* Distributed execution

---

# License

MIT License
