FROM golang:1.21 as builder

RUN apt-get update && apt-get install -y librdkafka-dev
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN go build -o main .


CMD ["./main"]