# Multi-arch build: docker buildx sets BUILDPLATFORM/TARGETOS/TARGETARCH so
# the (fast, native) builder cross-compiles for the requested target.
FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -ldflags="-s -w -X main.version=${VERSION}" -o /gateway ./cmd/gateway

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /gateway /app/gateway
COPY --from=builder /app/config /app/config

EXPOSE 8080

ENTRYPOINT ["/app/gateway"]
