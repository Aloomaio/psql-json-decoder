MODULE_big = json_decoder
EXTENSION = json_decoder

OBJS = logdecoder.o oid_util.o format-json.o snapshot-json.o

PG_CPPFLAGS += -std=c99

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
