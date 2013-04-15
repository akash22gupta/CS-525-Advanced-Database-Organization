OBJ=storage_mgr.o
TEST_OBJ=test_assign1_1.o
TEST_BIN=test_assign1
CFLAGS:=$(CFLAGS) -I. -g

all: $(TEST_BIN) 

$(TEST_BIN): $(OBJ) $(TEST_OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(TEST_OBJ)

clean:
	rm -f $(TEST_BIN) $(OBJ) $(TEST_OBJ)