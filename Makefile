BIN_FILE=kvbench
BIN_DIR=bin
SCRIPTS_DIR=scripts
BENCHMARKS_DIR=benchmarks

default: build

build:
	@go build -o $(BIN_DIR)/$(BIN_FILE) ./cmd/...

docker_build:
	docker build -t go-kvbench .

test:
	@go test -cover -race -v ./internal/providers

bench: clean build
	@$(SCRIPTS_DIR)/bench.sh
	@$(SCRIPTS_DIR)/report.sh

docker_bench:
	docker run -v $(BENCHMARKS_DIR):/code/$(BENCHMARKS_DIR) -it go-kvbench

clean:
	@rm -rf $(BIN_DIR)
	@rm -rf $(BENCHMARKS_DIR)/*
	@rm -rf .*db
	@rm -rf *.db
	@rm -rf pogreb.*
	@rm -rf *.tmp
