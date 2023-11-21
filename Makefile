SRCS=$(wildcard src/*.c)
SOBJ=$(SRCS:.c=.$(LIB_EXTENSION))
LUASRCS=$(shell find lib -name '*.lua')
INST_LUALIBDIR=$(INST_LUADIR)/postgres
LUALIBS=$(patsubst lib/%,$(INST_LUALIBDIR)/%,$(LUASRCS))

INSTALL:=$(shell which install)
ifndef INSTALL
$(error "install command is not available in your PATH")
endif

ifdef POSTGRES_COVERAGE
COVFLAGS=--coverage
endif

.PHONY: all install

all: $(SOBJ)

%.o: %.c
	$(CC) $(CFLAGS) $(WARNINGS) $(COVFLAGS) $(CPPFLAGS) -o $@ -c $<

%.$(LIB_EXTENSION): %.o
	$(CC) -o $@ $^ $(LDFLAGS) $(PLATFORM_LDFLAGS) $(COVFLAGS)

$(INST_LUALIBDIR)/%: lib/%
	@mkdir -p $(@D)
	@echo "install $< $@"
	@install $< $@

install: $(LUALIBS)
	$(INSTALL) -d $(INST_LIBDIR)/postgres/
	$(INSTALL) $(SOBJ) $(INST_LIBDIR)/postgres/
	rm -f $(SOBJ) src/*.gcda
