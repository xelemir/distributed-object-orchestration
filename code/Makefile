.PHONY: build run tidy test clean

build:
	go build ./...

run:
	go run ./cmd/dito

tidy:
	go mod tidy

test:
	go test ./...

clean:
	rm -rf ./data
