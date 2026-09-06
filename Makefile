.PHONY: build deps tidy generate-crd test test-cluster test-redis race-cluster race-redis differential-redis bench-redis scale-redis soak-cluster soak-redis vet lint fmt run docker-build

deps:
	go mod download

tidy:
	go mod tidy

generate-crd:
	go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.21.0 object paths=./operator/api/... crd paths=./operator/api/... output:crd:artifacts:config=operator/manifests

build:
	go build -o bin/pirindb ./cmd/pirindb
	go build -o bin/pirin-cli ./cmd/pirin-cli
	go build -o bin/pirindb-operator ./cmd/pirindb-operator

test:
	go test ./...

test-cluster:
	go test ./cmd/pirindb ./operator/controllers -run 'Cluster|Rebalance|Reconcile'

test-redis:
	go test ./cmd/pirindb ./storage -count=1

race-redis:
	go test -race ./cmd/pirindb ./storage -count=1

differential-redis:
	go test ./cmd/pirindb -run TestRedisSupportedSubsetAgainstReference -count=1

bench-redis:
	go test ./cmd/pirindb -run '^$$' -bench 'BenchmarkRedis(String|Raw|List|StandardPipeline)' -benchmem

scale-redis:
	PIRINDB_REDIS_SCALE_TEST=1 go test ./cmd/pirindb -run '^TestRedisMillionCompositeKeysScaleGate$$' -count=1 -v -timeout=30m

race-cluster:
	go test -race ./cmd/pirindb ./operator/controllers -run 'Cluster|Rebalance|Reconcile'

fuzz-cluster:
	go test ./cmd/pirindb -run '^$$' -fuzz FuzzClusterTransferFrameDecoder -fuzztime 10s

soak-cluster:
	bash hack/kind-soak.sh

soak-redis:
	bash hack/redis-soak.sh

vet:
	go vet ./...

lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

run:
	go run ./cmd/...

docker-build:
	docker build --target pirindb -t pirindb:dev .
	docker build --target operator -t pirindb-operator:dev .
