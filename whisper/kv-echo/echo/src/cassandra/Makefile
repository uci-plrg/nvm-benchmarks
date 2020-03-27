CC 	= gcc
INCL = -I./ -I$(HOME)/include/thrift/ -I/usr/include/boost/
GLIBCFLAGS1 := $(shell pkg-config --cflags glib-2.0)
GLIBCFLAGS2 := $(shell pkg-config --cflags gthread-2.0)
GLIBCFLAGS3 := $(shell pkg-config --cflags gobject-2.0)
GLIBCLIBS1 := $(shell pkg-config --libs glib-2.0)
GLIBCLIBS2 := $(shell pkg-config --libs gthread-2.0)
GLIBCLIBS3 := $(shell pkg-config --libs gobject-2.0)
CFLAGS 	= $(INCL) -D_GNU_SOURCE $(GLIBCFLAGS1) $(GLIBCFLAGS2) $(GLIBCFLAGS3) -g -Wall
#LIBS = -L$(HOME)/lib/ -lthrift -lthrift_c_glib -lthriftnb -lthriftz $(GLIBCLIBS1) $(GLIBCLIBS2) $(GLIBCLIBS3)
LIBS = -L$(HOME)/lib/ -lthrift -lthrift_c_glib $(GLIBCLIBS1) $(GLIBCLIBS2) $(GLIBCLIBS3)
#/home/pjh/lib/libthrift.a  /home/pjh/lib/libthrift_c_glib.a  /home/pjh/lib/libthriftnb.a  /home/pjh/lib/libthriftz.a
#   looks like libthriftnb needs -levent as well
#  Glib flags: http://developer.gnome.org/glib/2.28/glib-compiling.html
#    Note that if you want to override the system-default lib directories
#    (i.e. on a systems lab machine) and use your own, LD_RUN_PATH must
#    be set (in addition to using the pkg-config command below)
#    Example (before "make"): export LD_RUN_PATH=$HOME/lib
#  Also: http://stackoverflow.com/questions/1435861/computing-makefile-variable-on-assignment
#    http://linuxmafia.com/faq/Admin/ld-lib-path.html
#    http://gcc.gnu.org/faq.html

default: test

cassandra_types: cassandra_types.c cassandra_types.h
	$(CC) -c -o $@.o $@.c $(CFLAGS)

cassandra: cassandra.c cassandra.h
	$(CC) -c -o $@.o $@.c $(CFLAGS)

test-cassandra: test-cassandra.c
	$(CC) -c -o $@.o $@.c $(CFLAGS)

TEST_EXEC = test-cassandra
test: cassandra_types.o cassandra.o test-cassandra.o
#	$(CC) -o $(TEST_EXEC) $(CFLAGS) $^
#	$(CC) -o $(TEST_EXEC) $^ $(CFLAGS)
	$(CC) -o $(TEST_EXEC) test-cassandra.o cassandra.o cassandra_types.o $(CFLAGS) $(LIBS)
#	$(CC) -o $(TEST_EXEC) $(CFLAGS) $^
#	$(CC) -o $(TEST_EXEC) $(CFLAGS) test-cassandra.c
#$@: test
#$^: test-cassandra.c cassandra.c cassandra.h cassandra_types.c cassandra_types.h

all: test

clean:
	rm -f *.o test-cassandra

