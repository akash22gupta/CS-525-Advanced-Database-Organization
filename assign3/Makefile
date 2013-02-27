OBJ=expr.o dberror.o rm_serializer.o record_mgr.o
TEST_BIN=test_expr.bin
TEST_OBJ=$(TEST_BIN:.bin=.o)
CFLAGS:=$(CFLAGS) -I. -g

all: $(TEST_BIN) $(OBJ) $(TEST_OBJ)

%.bin: %.o $(OBJ) $(TEST_OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(@:.bin=.o)

clean:
	rm -f $(TEST_BIN) $(OBJ) $(TEST_OBJ)

.PHONY: clean