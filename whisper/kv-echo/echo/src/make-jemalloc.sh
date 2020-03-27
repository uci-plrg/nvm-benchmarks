#!/bin/bash

START_DIR="`pwd -P`"
MALLOC_DIR="${START_DIR}/malloc"
JEMALLOC_DIR="${MALLOC_DIR}/jemalloc-2.2.5"
MALLOC_LIBS="${MALLOC_DIR}/lib"

# clean:
rm -f ${MALLOC_LIBS}/libjemalloc*

cd ${JEMALLOC_DIR}
make distclean
./configure --prefix=${MALLOC_DIR}
make
make install

exit 0


