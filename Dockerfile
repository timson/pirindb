# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/pirindb ./cmd/pirindb
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags "-s -w" -o /out/pirindb-operator ./cmd/pirindb-operator

FROM scratch AS pirindb
COPY --from=builder /out/pirindb /usr/local/bin/pirindb
USER 65532:65532
VOLUME ["/var/lib/pirindb"]
EXPOSE 4321 6379
ENTRYPOINT ["/usr/local/bin/pirindb"]

FROM scratch AS operator
COPY --from=builder /out/pirindb-operator /usr/local/bin/pirindb-operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/pirindb-operator"]
