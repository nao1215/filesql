.PHONY: test clean vet fmt chkfmt

APP         = filesql
VERSION     = $(shell git describe --tags --abbrev=0)
GO          = go
GO_BUILD    = $(GO) build
GO_FORMAT   = $(GO) fmt
GOFMT       = gofmt
GO_LIST     = $(GO) list
GO_TEST     = $(GO) test
GO_TOOL     = $(GO) tool
GO_VET      = $(GO) vet
GO_DEP      = $(GO) mod
GO_INSTALL  = $(GO) install
GOOS        = ""
GOARCH      = ""
GO_PKGROOT  = ./...
GO_PACKAGES = $(shell $(GO_LIST) $(GO_PKGROOT))
GO_LDFLAGS  = -ldflags '-X github.com/nao1215/sqluv/config.Version=${VERSION}'


clean: ## Clean project
	-rm -rf $(APP) cover.*

test: ## Start test
	env GOOS=$(GOOS) $(GO_TEST) -cover $(GO_PKGROOT) -coverpkg=./... -coverprofile=cover.out
	$(GO_TOOL) cover -html=cover.out -o cover.html

gen: ## Generate code from templates
	$(GO) generate ./...

tools: ## Install dependency tools 
	$(GO_INSTALL) github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
	$(GO_INSTALL) github.com/k1LoW/octocov@latest

lint: ## Lint code
	golangci-lint run --config .golangci.yml

.DEFAULT_GOAL := help
help:  
	@grep -E '^[0-9a-zA-Z_-]+[[:blank:]]*:.*?## .*$$' $(MAKEFILE_LIST) | sort \
	| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[1;32m%-15s\033[0m %s\n", $$1, $$2}'
