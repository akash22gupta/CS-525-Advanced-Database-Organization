CC=clang
CFLAGS=-std=c99 -I.

DEPS = dberror.h storage_mgr.h test_helper.h buffer_mgr_stat.h buffer_mgr.h 
OBJ1 = dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o test_assign2_1.o
OBJ2 = dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o test_assign2_2.o

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

test_assign2_1: $(OBJ1)
	cc -o  $@ $^ $(CFLAGS)

test_assign2_2: $(OBJ2)
	cc -o  $@ $^ $(CFLAGS)

clean:
	rm -f $(OBJ1)
	rm -f $(OBJ1)
