FROM golang:1.16-alpine

ENV LOG_LEVEL="info"

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY main.go main.go
COPY backup/ backup/
COPY util/ util/

RUN go build -o /operator-agent

EXPOSE 8080

CMD [ "/operator-agent" ]