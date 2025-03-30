test:
	go test -v ./...

deps:
	go mod tidy

build: deps
	go build -o bin/pirindb ./cmd/pirindb
	go build -o bin/pirin-cli ./cmd/pirin-cli
vet:
	go vet ./...

lint:
	golangci-lint run ./...

fmt:
	go fmt ./...

run:
	go run ./cmd/...
