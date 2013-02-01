OBJ=storage_mgr.o buffer_mgr.o buffer_mgr_stat.o dberror.o
TEST_OBJ=test_assign2_1.o
TEST_BIN=test_assign2
CFLAGS:=$(CFLAGS) -I. -g

all: $(TEST_BIN) 

$(TEST_BIN): $(OBJ) $(TEST_OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(TEST_OBJ)

clean:
	rm -f $(TEST_BIN) $(OBJ) $(TEST_OBJ)