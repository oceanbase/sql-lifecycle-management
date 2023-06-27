default: build

GO_TOOLS_BIN_PATH := $(shell pwd)/.tools/bin
PATH := $(GO_TOOLS_BIN_PATH):$(PATH)

install-tools:
	@ echo "install-tools ..."
	@ mkdir -p $(GO_TOOLS_BIN_PATH)
	@ grep '_' toolset/toolset.go | sed 's/"//g' | awk '{print $$2}' | xargs go install

ALL_PKG := github.com/oceanbase/sql-lifecycle-management/agent
PACKAGES := $(shell go list ./...)
PACKAGE_DIRECTORIES := $(subst $(ALL_PKG)/,,$(PACKAGES))
PACKAGES_WITHOUT_TOOLSET := $(shell go list ./... | sed '/^github.com\/oceanbase\/sql-lifecycle-management\/agent\/toolset/d')

LDFLAGS = -s -w

check: install-tools
	@ echo "do checks ..."
	@ echo "gofmt ..."
	@ gofmt -s -l -w $(PACKAGE_DIRECTORIES)
	@ echo "golines ..."
	@ golines --max-len=100 --shorten-comments -w cmd pkg
	@ echo "revive ..."
	@ revive -formatter friendly -config revive.toml $(PACKAGES_WITHOUT_TOOLSET)

test: install-tools
	@ echo "unit test ..."
	@ go test -timeout 5m -race -cover $(PACKAGES_WITHOUT_TOOLSET)

build: check
	@ echo "building ..."
	@ mkdir -p ./bin
	@ go build -ldflags="$(LDFLAGS)" -o ./bin/sqless-agent ./cmd/...

clean:
	@ echo "clean ..."
	@ rm -f ./bin/sqless-agent
