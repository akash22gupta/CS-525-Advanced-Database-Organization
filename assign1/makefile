CC=clang
CFLAGS=-std=c99 -I.
DEPS = dberror.h storage_mgr.h test_helper.h
OBJ = dberror.o storage_mgr.o test_assign1_1.o
OBJ1 = dberror.o storage_mgr.o test_assign1_2.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign1: $(OBJ)
	gcc -o  $@ $^ $(CFLAGS)

test_assign2: $(OBJ1)
	gcc -o  $@ $^ $(CFLAGS)


clean:
	rm -f $(OBJ)
	rm -f $(OBJ1)