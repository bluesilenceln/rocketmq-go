WORKROOT := $(shell pwd)
OUTDIR := $(WORKROOT)/output

GO := go
GOBUILD := $(GO) build

all: build

build:
	mkdir -p $(OUTDIR)/bin $(OUTDIR)/conf
	$(GOBUILD) -o $(OUTDIR)/bin
	cp config/namesrv.toml $(OUTDIR)/conf/

clean:
	rm -rf $(OUTDIR)

.PHONY: clean
