OBJ=expr.o dberror.o rm_serializer.o record_mgr.o
HEADERS=buffer_mgr.h dberror.h expr.h record_mgr.h storage_mgr.h tables.h test_helper.h
TEST_BIN=test_expr.bin test_assign3_1.bin
TEST_OBJ=$(TEST_BIN:.bin=.o)
CFLAGS:=$(CFLAGS) -I. -g -Wall -Werror

all: $(TEST_BIN) $(OBJ) $(TEST_OBJ) $(HEADERS)

$(OBJ): $(HEADERS)

%.bin: %.o $(OBJ) $(TEST_OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(@:.bin=.o)

clean:
	rm -f $(TEST_BIN) $(OBJ) $(TEST_OBJ)

.PHONY: clean