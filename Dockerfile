FROM golang:1.19 AS builder

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY . ./

RUN GOOS=linux GOARCH=amd64 go build -v -o platform-operator-agent

FROM registry.access.redhat.com/ubi8/ubi-minimal:8.5

ENV LOG_LEVEL="info"

WORKDIR /

COPY --from=builder /app/platform-operator-agent /platform-operator-agent

EXPOSE 8080

USER 65532:65532

ENTRYPOINT ["/platform-operator-agent"]
