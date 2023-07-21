SRCS=$(wildcard src/*.c)
OBJS=$(SRCS:.c=.o)
SOFILES=$(OBJS:.o=.$(LIB_EXTENSION))
CLIBS=src/pgconn.so src/misc.so src/util.so
LUASRCS=$(shell find lib -name '*.lua')
INST_LUALIBDIR=$(INST_LUADIR)/postgres
LUALIBS=$(patsubst lib/%,$(INST_LUALIBDIR)/%,$(LUASRCS))

PG_CONFIG:=$(shell which pg_config)
ifndef PG_CONFIG
$(error "pg_config command is not available in your PATH")
endif
LIBPQ_INCDIR:=$(shell pg_config --includedir)
LIBPQ_LIBDIR:=$(shell pg_config --libdir)

INSTALL:=$(shell which install)
ifndef INSTALL
$(error "install command is not available in your PATH")
endif

ifdef POSTGRES_COVERAGE
COVFLAGS=--coverage
endif

.PHONY: all install

all: $(CLIBS)

%.o: %.c
	$(CC) $(CFLAGS) $(WARNINGS) $(COVFLAGS) $(CPPFLAGS) -I$(LIBPQ_INCDIR) -o $@ -c $<

src/pgconn.$(LIB_EXTENSION): src/pgconn.o src/pgresult.o src/pgcancel.o
	$(CC) -o $@ $^ $(LDFLAGS) -L$(LIBPQ_LIBDIR) -lpq $(LIBS) $(PLATFORM_LDFLAGS) $(COVFLAGS)

src/misc.$(LIB_EXTENSION): src/misc.o
	$(CC) -o $@ $^ $(LDFLAGS) -L$(LIBPQ_LIBDIR) -lpq $(LIBS) $(PLATFORM_LDFLAGS) $(COVFLAGS)

src/util.$(LIB_EXTENSION): src/util.o
	$(CC) -o $@ $^ $(LDFLAGS) -L$(LIBPQ_LIBDIR) -lpq $(LIBS) $(PLATFORM_LDFLAGS) $(COVFLAGS)

$(INST_LUALIBDIR)/%: lib/%
	@mkdir -p $(@D)
	@echo "install $< $@"
	@install $< $@

install: $(LUALIBS)
	$(INSTALL) -d $(INST_LIBDIR)/postgres/
	$(INSTALL) $(CLIBS) $(INST_LIBDIR)/postgres/
	rm -f $(OBJS) $(CLIBS) src/*.gcda
