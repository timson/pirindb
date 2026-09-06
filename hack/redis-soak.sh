#!/usr/bin/env bash
set -euo pipefail

duration_seconds="${SOAK_DURATION_SECONDS:-300}"
deadline=$((SECONDS + duration_seconds))
iteration=0

while (( SECONDS < deadline )); do
  iteration=$((iteration + 1))
  echo "redis soak iteration ${iteration}"
  go test ./cmd/pirindb -run 'TestRedis(ActiveExpiry|LegacyHashMigrates|MigrationRestart|LargeList|LargeZSet|StandardPipeline|LogicalIntegrity|Flush|Bloom|TopK|GroupPolicy|AbandonedBuild|GenerationGC|ExactKeyGC)' -count=1
  go test ./storage -run 'Test(CommitAsync|GroupPolicy|BTreeRandomized|Freelist)' -count=1
done

echo "redis soak completed ${iteration} iterations"
