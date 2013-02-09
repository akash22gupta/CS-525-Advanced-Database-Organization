OBJ=storage_mgr.o buffer_mgr.o buffer_mgr_stat.o dberror.o
TEST_BIN=test_assign2_1.bin test_assign2_2.bin
CFLAGS:=$(CFLAGS) -I. -g

all: $(TEST_BIN)

%.bin: %.o $(OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(@:.bin=.o)

clean:
	rm -f $(TEST_BIN) $(OBJ) $(TEST_BIN:.bin=.o)