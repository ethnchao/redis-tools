BINARY_NAME=redis-tools

build:
	GOARCH=amd64 GOOS=linux go build -o ${BINARY_NAME}-linux ./

build-all:
	GOARCH=amd64 GOOS=darwin go build -o ${BINARY_NAME}-darwin ./
	GOARCH=arm64 GOOS=darwin go build -o ${BINARY_NAME}-darwin-arm64 ./
	GOARCH=amd64 GOOS=linux go build -o ${BINARY_NAME}-linux ./
	GOARCH=amd64 GOOS=windows go build -o ${BINARY_NAME}-windows ./

run: build
	./${BINARY_NAME}

clean:
	go clean
	rm ${BINARY_NAME}-darwin
	rm ${BINARY_NAME}-linux
	rm ${BINARY_NAME}-windows

test:
	go test ./...

test_coverage:
	go test ./... -coverprofile=coverage.out

dep:
	go mod download

vet:
	go vet

lint:
	golangci-lint run --enable-all

copy:
	kubectl cp redis-tools-linux $(pod):/opt/redis-tools
