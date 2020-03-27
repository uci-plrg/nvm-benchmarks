INCL = -I./

CC 	= gcc
CFLAGS 	= $(INCL) -D_GNU_SOURCE -g -Wall -Wunused

default: test_hash_table
ALL = test_hash_table
all: $(ALL)

######################################################
HEADERS: bitrotate.h hash.h xalloc-oversized.h

hash_table.o: hash.c $(HEADERS)
	$(CC) -c -o $@ $< $(CFLAGS)

HT_OBJ	= hash_table.o

test_hash_table: test_hash_table.c $(HT_OBJ)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

######################################################
clean:
	rm -f *.o $(ALL)

