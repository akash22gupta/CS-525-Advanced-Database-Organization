OBJ=
BIN=bpt.bin
CFLAGS:=$(CFLAGS) -I. -g -Wall -Werror

all: $(BIN) $(OBJ) 

%.bin: %.o $(OBJ)
	$(CC) $(CFLAGS) -o $@ $(OBJ) $(@:.bin=.o)

clean:
	rm -f $(BIN) $(OBJ)

.PHONY: clean