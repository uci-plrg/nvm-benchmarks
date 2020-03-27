# Peter Hornyack
# 2012-06-23
#
# Everything in this Makefile is kind of hacky.

CC 		= gcc
CFLAGS 	= -g -Wall -Wunused
LIBS	= -lpthread

default: test_threadpool
ALL = test_threadpool
all: $(ALL)

######################################################

KP_STUFF			= ../kp_common.h ../kp_common.c ../kp_recovery.h ../kp_recovery.c
QUEUE_STUFF			= queue.c queue.h
THREADPOOL_STUFF	= threadpool.c threadpool.h

queue.o: $(QUEUE_STUFF) $(KP_STUFF) threadpool_macros.h
	$(CC) -c queue.c -o $@ $(CFLAGS)

threadpool.o: $(THREADPOOL_STUFF) threadpool_macros.h
	$(CC) -c threadpool.c -o $@ $(CFLAGS)

TP_OBJ	= queue.o threadpool.o

test_threadpool: test_threadpool.c $(TP_OBJ)
	$(CC) -o $@ $^ $(KP_STUFF) $(CFLAGS) $(LIBS)

clean:
	rm -f *.o $(ALL) tags

