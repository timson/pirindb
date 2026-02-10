# ORM Dataset Playground

Small manual playground for testing ORM queries and speed on a real dataset.

Dataset used by default:
- Source: `vega-datasets` cars dataset
- URL: `https://raw.githubusercontent.com/vega/vega-datasets/main/data/cars.json`

The loader maps each JSON document to a `Car` ORM model and stores it in PirinDB.
It can also expand the real dataset deterministically to large sizes (for example 1,000,000 rows).

## Quick Start

1. Prepare dataset and load DB (1,000,000 rows by default):

```bash
go run ./playground/orm_dataset -prepare
```

Or explicitly:

```bash
go run ./playground/orm_dataset -prepare -target-rows 1000000
```

2. Run one query:

```bash
go run ./playground/orm_dataset \
  -origin USA \
  -min-hp 150 \
  -limit 20 \
  -print 10
```

3. Benchmark repeated query execution:

```bash
go run ./playground/orm_dataset \
  -origin Japan \
  -min-hp 90 \
  -page 1 -page-size 25 \
  -bench-runs 2000
```

## Boolean Query Shape

If `-not-origin` is set:
- default shape: `(A and B) and (not C)`
- with `-or-not`: `(A and B) or (not C)`

Where:
- `A`: `Origin == -origin`
- `B`: `Horsepower >= -min-hp`
- `C`: `Origin == -not-origin`

## Useful Flags

- `-prepare` download/load dataset
- `-redownload` force dataset redownload
- `-reset-db` reset DB before loading (default `true`)
- `-dataset` local dataset JSON path
- `-db` DB path
- `-batch-size` loader batch size
- `-target-rows` how many rows to load (default `1000000`, `0` = original size only)
- `-limit`, `-offset`
- `-page`, `-page-size` (pagination mode)
- `-bench-runs` repeated query benchmark
- `-print` how many rows to print
