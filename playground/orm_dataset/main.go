package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/timson/pirindb/orm"
	"github.com/timson/pirindb/storage"
)

type queryOptions struct {
	origin    string
	minHP     int
	notOrigin string
	orNot     bool
	limit     int
	offset    int
	page      int
	pageSize  int
}

func main() {
	var (
		prepare     = flag.Bool("prepare", false, "Download dataset (if needed) and load it into DB")
		redownload  = flag.Bool("redownload", false, "Force redownload dataset file")
		resetDB     = flag.Bool("reset-db", true, "Reset DB files before loading dataset")
		datasetURL  = flag.String("dataset-url", defaultDatasetURL, "Dataset URL to download")
		datasetPath = flag.String("dataset", defaultDatasetPath, "Path to local dataset JSON")
		dbPath      = flag.String("db", defaultDBPath, "Path to local DB file")
		batchSize   = flag.Int("batch-size", 128, "Batch size for loading dataset")
		targetRows  = flag.Int("target-rows", 1_000_000, "How many rows to load; 0 means original dataset size")

		origin    = flag.String("origin", "USA", "Origin filter (A)")
		minHP     = flag.Int("min-hp", 120, "Minimum horsepower filter (B)")
		notOrigin = flag.String("not-origin", "", "Origin for NOT condition (C)")
		orNot     = flag.Bool("or-not", false, "If set, query is (A and B) or (not C). Otherwise (A and B) and (not C) when C is provided")

		page     = flag.Int("page", 1, "Page number (1-based), used when page-size > 0")
		pageSize = flag.Int("page-size", 0, "Page size; 0 disables page mode")
		limit    = flag.Int("limit", 20, "Limit for query result (-1 means unlimited)")
		offset   = flag.Int("offset", 0, "Offset for query result")

		benchRuns = flag.Int("bench-runs", 0, "How many times to execute the same query for timing")
		printN    = flag.Int("print", 10, "How many rows to print from one query")
	)
	flag.Parse()

	ctx := context.Background()

	if *prepare {
		if err := ensureDatasetFile(ctx, *datasetURL, *datasetPath, *redownload); err != nil {
			fatalf("prepare: dataset download failed: %v", err)
		}
		if *resetDB {
			if err := resetDBFiles(*dbPath); err != nil {
				fatalf("prepare: reset db failed: %v", err)
			}
		}
		db, err := storage.Open(*dbPath, nil)
		if err != nil {
			fatalf("prepare: open db failed: %v", err)
		}

		start := time.Now()
		n, err := loadCarsDataset(db, *datasetPath, *batchSize, *targetRows)
		if err != nil {
			_ = db.Close()
			fatalf("prepare: load dataset failed: %v", err)
		}
		if err = db.Close(); err != nil {
			fatalf("prepare: close db failed: %v", err)
		}
		fmt.Printf("Loaded %d rows into %s in %s\n", n, *dbPath, time.Since(start))
	}

	if _, err := os.Stat(*dbPath); err != nil {
		if os.IsNotExist(err) {
			fatalf("db file %s not found. Run with -prepare first", *dbPath)
		}
		fatalf("db path check failed: %v", err)
	}

	db, err := storage.Open(*dbPath, nil)
	if err != nil {
		fatalf("open db failed: %v", err)
	}
	defer func() { _ = db.Close() }()

	o := orm.New(db)
	q := buildQuery(queryOptions{
		origin:    *origin,
		minHP:     *minHP,
		notOrigin: *notOrigin,
		orNot:     *orNot,
		limit:     *limit,
		offset:    *offset,
		page:      *page,
		pageSize:  *pageSize,
	})

	if *benchRuns > 0 {
		runBenchmark(o, q, *benchRuns)
		return
	}

	runSingleQuery(o, q, *printN)
}

func buildQuery(opts queryOptions) *orm.Query {
	base := orm.And(
		orm.Where("Origin").Eq(opts.origin),
		orm.Where("Horsepower").Gte(opts.minHP),
	)

	if opts.notOrigin != "" {
		notExpr := orm.Not(orm.Where("Origin").Eq(opts.notOrigin))
		if opts.orNot {
			base = base.Or(notExpr)
		} else {
			base = base.And(notExpr)
		}
	}

	if opts.pageSize > 0 {
		return base.Page(opts.page, opts.pageSize)
	}
	if opts.offset > 0 {
		base = base.Offset(opts.offset)
	}
	if opts.limit >= 0 {
		base = base.Limit(opts.limit)
	}
	return base
}

func runSingleQuery(o *orm.ORM, q *orm.Query, printN int) {
	start := time.Now()
	rows, err := orm.FindAll[Car](o, q)
	elapsed := time.Since(start)
	if err != nil {
		fatalf("query failed: %v", err)
	}

	fmt.Printf("Query returned %d rows in %s\n", len(rows), elapsed)
	if printN <= 0 {
		return
	}
	limit := printN
	if limit > len(rows) {
		limit = len(rows)
	}
	for i := 0; i < limit; i++ {
		row := rows[i]
		fmt.Printf(
			"%3d) id=%d name=%q origin=%s hp=%d cyl=%d year=%d mpg=%.1f\n",
			i+1, row.ID, row.Name, row.Origin, row.Horsepower, row.Cylinders, row.Year, row.MPG,
		)
	}
}

func runBenchmark(o *orm.ORM, q *orm.Query, runs int) {
	start := time.Now()
	lastResultN := 0
	for i := 0; i < runs; i++ {
		rows, err := orm.FindAll[Car](o, q)
		if err != nil {
			fatalf("benchmark query failed on run %d: %v", i+1, err)
		}
		lastResultN = len(rows)
	}
	total := time.Since(start)
	avg := total / time.Duration(runs)
	qps := float64(runs) / total.Seconds()

	fmt.Printf(
		"Benchmark runs=%d total=%s avg=%s qps=%.2f last_result_rows=%d\n",
		runs, total, avg, qps, lastResultN,
	)
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
