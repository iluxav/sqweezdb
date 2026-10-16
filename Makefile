# sqweezdb: build, install and package the `sqweezdb` command.
#
#   make                      build target/dist/sqweezdb for this machine
#   make install              install to ~/.local/bin (or /usr/local/bin as root); PREFIX=/opt/x to change
#   make uninstall            remove it again (same PREFIX)
#   make test                 unit tests
#   make dist                 dist/sqweezdb-<target>.tar.gz + .sha256 (what a release ships)
#   make dist TARGET=x86_64-unknown-linux-gnu GLIBC=2.17 CARGO_BUILD="cargo zigbuild"   (runs on glibc >= 2.17)
#   make lab                  every experiment, including the ones that race SQLite and Turso
#   make bindings             the Node, Python and Go bindings

BIN         := sqweezdb
VERSION     := $(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)
CARGO       ?= cargo
CARGO_BUILD ?= $(CARGO) build
TARGET      ?=
GLIBC       ?=
HOST        := $(shell rustc -vV 2>/dev/null | sed -n 's/^host: //p')
TRIPLE      := $(if $(TARGET),$(TARGET),$(HOST))
OUT         := target/$(if $(TARGET),$(TARGET)/)dist/$(BIN)
PREFIX      ?= $(if $(filter 0,$(shell id -u)),/usr/local,$(HOME)/.local)
BINDIR      ?= $(PREFIX)/bin
DESTDIR     ?=
INSTALLED   := $(DESTDIR)$(BINDIR)/$(BIN)

.PHONY: all build install uninstall test dist lab bindings node python go clean help version
all: build

build:
	$(CARGO_BUILD) --profile dist --bin $(BIN) $(if $(TARGET),--target $(TARGET)$(if $(GLIBC),.$(GLIBC)))
	@echo "built $(OUT) ($(BIN) $(VERSION), $(TRIPLE))"

install: build
	install -d "$(DESTDIR)$(BINDIR)"
	install -m 0755 "$(OUT)" "$(INSTALLED)"
	@echo "installed $(INSTALLED)"
	@case ":$$PATH:" in *":$(BINDIR):"*) ;; *) echo "note: $(BINDIR) is not on your PATH; add it, e.g. export PATH=\"$(BINDIR):\$$PATH\"";; esac

uninstall:
	@if [ -e "$(INSTALLED)" ]; then rm -f "$(INSTALLED)" && echo "removed $(INSTALLED)"; else echo "nothing to remove at $(INSTALLED)"; fi

test:
	$(CARGO) test --release --lib

# The release artifact: a tarball named without the version, so .../releases/latest/download/<name> always works.
dist: build
	@rm -rf "dist/$(BIN)-$(TRIPLE)" && mkdir -p "dist/$(BIN)-$(TRIPLE)"
	cp "$(OUT)" README.md "dist/$(BIN)-$(TRIPLE)/"
	tar -C dist -czf "dist/$(BIN)-$(TRIPLE).tar.gz" "$(BIN)-$(TRIPLE)"
	@rm -rf "dist/$(BIN)-$(TRIPLE)"
	cd dist && (sha256sum "$(BIN)-$(TRIPLE).tar.gz" 2>/dev/null || shasum -a 256 "$(BIN)-$(TRIPLE).tar.gz") > "$(BIN)-$(TRIPLE).tar.gz.sha256"
	@echo "dist/$(BIN)-$(TRIPLE).tar.gz"; cat "dist/$(BIN)-$(TRIPLE).tar.gz.sha256"

lab:
	$(CARGO) build --release --features lab --bins

bindings: node python go
node:
	cd bindings/node && $(CARGO) build --release && cp target/release/libsqweezdb_node.so sqweezdb.node
python:
	cd bindings/python && $(CARGO) build --release && cp target/release/libsqweezdb_py.so sqweezdb.so
go:
	$(CARGO) build --release --lib
	cd bindings/go && go build ./...

version:
	@echo $(VERSION)

clean:
	$(CARGO) clean
	rm -rf dist

help:
	@sed -n 's/^#   //p' Makefile
