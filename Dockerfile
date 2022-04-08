FROM golang:1.16 AS builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY main.go main.go
COPY backup/ backup/
COPY util/ util/

RUN go build -o /operator-agent

FROM registry.access.redhat.com/ubi8/ubi-minimal:8.5

ENV LOG_LEVEL="info"

WORKDIR /

COPY --from=builder /operator-agent /operator-agent

EXPOSE 8080

ENTRYPOINT ["/operator-agent"]