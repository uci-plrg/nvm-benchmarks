#!/bin/bash

START_DIR="`pwd -P`"
MALLOC_DIR="${START_DIR}/malloc"
GPERF_DIR="${MALLOC_DIR}/gperftools-2.0"
MALLOC_LIBS="${MALLOC_DIR}/lib"

# clean:
rm -f ${MALLOC_LIBS}/libtcmalloc*
rm -f ${MALLOC_LIBS}/pkgconfig/libprofiler*
rm -f ${MALLOC_LIBS}/pkgconfig/libtcmalloc*

cd ${GPERF_DIR}
make distclean
./configure --prefix=${MALLOC_DIR} --enable-minimal
make
make install

exit 0

#----------------------------------------------------------------------
#Libraries have been installed in:
#   /homes/sys/pjh/research/nvm/novaOS/keyvalue/src/malloc/lib
#
#If you ever happen to want to link against installed libraries
#in a given directory, LIBDIR, you must either use libtool, and
#specify the full pathname of the library, or use the `-LLIBDIR'
#flag during linking and do at least one of the following:
#   - add LIBDIR to the `LD_LIBRARY_PATH' environment variable
#     during execution
#   - add LIBDIR to the `LD_RUN_PATH' environment variable
#     during linking
#   - use the `-Wl,-rpath -Wl,LIBDIR' linker flag
#   - have your system administrator add LIBDIR to `/etc/ld.so.conf'
#
#See any operating system documentation about shared libraries for
#more information, such as the ld(1) and ld.so(8) manual pages.
#----------------------------------------------------------------------

