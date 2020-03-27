INCL = -I./

CC 	= gcc
CFLAGS 	= $(INCL) -O3 -g -Wall
#CFLAGS 	= $(INCL) -O3 -g -Wall -Wunused -Wextra

default: test_vector test_vector64
ALL = test_vector test_vector64
all: $(ALL)

######################################################
# This is pretty hacky...
kp_recovery.o: ../kp_recovery.c ../kp_recovery.h
	$(CC) -c -o $@ $< $(CFLAGS)

RECOVERY_OBJ	= kp_recovery.o

######################################################
vector.o: vector.c vector.h vector_macros.h ../kp_recovery.h
	$(CC) -c -o $@ $< $(CFLAGS)

VEC_OBJ		= vector.o

test_vector: test_vector.c $(VEC_OBJ) $(RECOVERY_OBJ)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

######################################################
vector64.o: vector64.c vector64.h vector_macros.h ../kp_recovery.h
	$(CC) -c -o $@ $< $(CFLAGS)

VEC_OBJ		= vector64.o

test_vector64: test_vector64.c $(VEC_OBJ) $(RECOVERY_OBJ)
	$(CC) -o $@ $^ $(CFLAGS) $(LIBS)

######################################################
clean:
	rm -f *.o $(ALL)

