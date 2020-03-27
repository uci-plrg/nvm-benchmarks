INCL = -I./

CC 	= gcc
CFLAGS 	= $(INCL) -g -Wall -Wunused

default: test_vector test_vector64
ALL = test_vector test_vector64
all: $(ALL)

######################################################
vector.o: vector.c vector.h vector_macros.h
	$(CC) -c -o $@ $< $(CFLAGS)

VEC_OBJ		= vector.o

test_vector: test_vector.c $(VEC_OBJ)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

######################################################
vector64.o: vector64.c vector64.h vector_macros.h
	$(CC) -c -o $@ $< $(CFLAGS)

VEC_OBJ		= vector64.o

test_vector64: test_vector64.c $(VEC_OBJ)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

######################################################
clean:
	rm -f *.o $(ALL)

