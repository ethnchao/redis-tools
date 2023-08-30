BINARY_NAME=redis-tools
DIST=dist

build:
	#GOARCH=amd64 GOOS=darwin go build -o ${DIST}/${BINARY_NAME}-darwin main.go
	GOARCH=amd64 GOOS=linux go build -o ${DIST}/${BINARY_NAME}-linux main.go
	#GOARCH=amd64 GOOS=windows go build -o ${DIST}/${BINARY_NAME}-windows main.go

run: build
	./${DIST}/${BINARY_NAME}

clean:
	go clean
	rm ${DIST}/${BINARY_NAME}-darwin
	rm ${DIST}/${BINARY_NAME}-linux
	rm ${DIST}/${BINARY_NAME}-windows

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