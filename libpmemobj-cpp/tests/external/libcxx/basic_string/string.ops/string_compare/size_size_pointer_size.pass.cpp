//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// Copyright 2019-2020, Intel Corporation
//
// Modified to test pmem::obj containers
//

#include "unittest.hpp"

#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

namespace nvobj = pmem::obj;

struct root {
	nvobj::persistent_ptr<pmem::obj::string> s1, s2, s3, s4;
};

int
sign(int x)
{
	if (x == 0)
		return 0;
	if (x < 0)
		return -1;
	return 1;
}

template <class S>
void
test(const S &s, typename S::size_type pos, typename S::size_type n1,
     const typename S::value_type *str, typename S::size_type n2, int x)
{
	if (pos <= s.size())
		UT_ASSERT(sign(s.compare(pos, n1, str, n2)) == sign(x));
	else {
		try {
			s.compare(pos, n1, str, n2);
			UT_ASSERT(0);
		} catch (std::out_of_range &) {
			UT_ASSERT(pos > s.size());
		}
	}
}

template <class S>
void
test0(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s1, 0, 0, "", 0, 0);
	test(*r->s1, 0, 0, "abcde", 0, 0);
	test(*r->s1, 0, 0, "abcde", 1, -1);
	test(*r->s1, 0, 0, "abcde", 2, -2);
	test(*r->s1, 0, 0, "abcde", 4, -4);
	test(*r->s1, 0, 0, "abcde", 5, -5);
	test(*r->s1, 0, 0, "abcdefghij", 0, 0);
	test(*r->s1, 0, 0, "abcdefghij", 1, -1);
	test(*r->s1, 0, 0, "abcdefghij", 5, -5);
	test(*r->s1, 0, 0, "abcdefghij", 9, -9);
	test(*r->s1, 0, 0, "abcdefghij", 10, -10);
	test(*r->s1, 0, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s1, 0, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s1, 0, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s1, 0, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s1, 0, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s1, 0, 1, "", 0, 0);
	test(*r->s1, 0, 1, "abcde", 0, 0);
	test(*r->s1, 0, 1, "abcde", 1, -1);
	test(*r->s1, 0, 1, "abcde", 2, -2);
	test(*r->s1, 0, 1, "abcde", 4, -4);
	test(*r->s1, 0, 1, "abcde", 5, -5);
	test(*r->s1, 0, 1, "abcdefghij", 0, 0);
	test(*r->s1, 0, 1, "abcdefghij", 1, -1);
	test(*r->s1, 0, 1, "abcdefghij", 5, -5);
	test(*r->s1, 0, 1, "abcdefghij", 9, -9);
	test(*r->s1, 0, 1, "abcdefghij", 10, -10);
	test(*r->s1, 0, 1, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s1, 0, 1, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s1, 0, 1, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s1, 0, 1, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s1, 0, 1, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s1, 1, 0, "", 0, 0);
	test(*r->s1, 1, 0, "abcde", 0, 0);
	test(*r->s1, 1, 0, "abcde", 1, 0);
	test(*r->s1, 1, 0, "abcde", 2, 0);
	test(*r->s1, 1, 0, "abcde", 4, 0);
	test(*r->s1, 1, 0, "abcde", 5, 0);
	test(*r->s1, 1, 0, "abcdefghij", 0, 0);
	test(*r->s1, 1, 0, "abcdefghij", 1, 0);
	test(*r->s1, 1, 0, "abcdefghij", 5, 0);
	test(*r->s1, 1, 0, "abcdefghij", 9, 0);
	test(*r->s1, 1, 0, "abcdefghij", 10, 0);
	test(*r->s1, 1, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s1, 1, 0, "abcdefghijklmnopqrst", 1, 0);
	test(*r->s1, 1, 0, "abcdefghijklmnopqrst", 10, 0);
	test(*r->s1, 1, 0, "abcdefghijklmnopqrst", 19, 0);
	test(*r->s1, 1, 0, "abcdefghijklmnopqrst", 20, 0);
	test(*r->s2, 0, 0, "", 0, 0);
	test(*r->s2, 0, 0, "abcde", 0, 0);
	test(*r->s2, 0, 0, "abcde", 1, -1);
	test(*r->s2, 0, 0, "abcde", 2, -2);
	test(*r->s2, 0, 0, "abcde", 4, -4);
	test(*r->s2, 0, 0, "abcde", 5, -5);
	test(*r->s2, 0, 0, "abcdefghij", 0, 0);
	test(*r->s2, 0, 0, "abcdefghij", 1, -1);
	test(*r->s2, 0, 0, "abcdefghij", 5, -5);
	test(*r->s2, 0, 0, "abcdefghij", 9, -9);
	test(*r->s2, 0, 0, "abcdefghij", 10, -10);
	test(*r->s2, 0, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s2, 0, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s2, 0, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s2, 0, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s2, 0, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s2, 0, 1, "", 0, 1);
	test(*r->s2, 0, 1, "abcde", 0, 1);
	test(*r->s2, 0, 1, "abcde", 1, 0);
	test(*r->s2, 0, 1, "abcde", 2, -1);
	test(*r->s2, 0, 1, "abcde", 4, -3);
	test(*r->s2, 0, 1, "abcde", 5, -4);
	test(*r->s2, 0, 1, "abcdefghij", 0, 1);
	test(*r->s2, 0, 1, "abcdefghij", 1, 0);
	test(*r->s2, 0, 1, "abcdefghij", 5, -4);
	test(*r->s2, 0, 1, "abcdefghij", 9, -8);
	test(*r->s2, 0, 1, "abcdefghij", 10, -9);
	test(*r->s2, 0, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s2, 0, 1, "abcdefghijklmnopqrst", 1, 0);
	test(*r->s2, 0, 1, "abcdefghijklmnopqrst", 10, -9);
	test(*r->s2, 0, 1, "abcdefghijklmnopqrst", 19, -18);
	test(*r->s2, 0, 1, "abcdefghijklmnopqrst", 20, -19);
	test(*r->s2, 0, 2, "", 0, 2);
	test(*r->s2, 0, 2, "abcde", 0, 2);
	test(*r->s2, 0, 2, "abcde", 1, 1);
	test(*r->s2, 0, 2, "abcde", 2, 0);
	test(*r->s2, 0, 2, "abcde", 4, -2);
	test(*r->s2, 0, 2, "abcde", 5, -3);
	test(*r->s2, 0, 2, "abcdefghij", 0, 2);
	test(*r->s2, 0, 2, "abcdefghij", 1, 1);
	test(*r->s2, 0, 2, "abcdefghij", 5, -3);
	test(*r->s2, 0, 2, "abcdefghij", 9, -7);
	test(*r->s2, 0, 2, "abcdefghij", 10, -8);
	test(*r->s2, 0, 2, "abcdefghijklmnopqrst", 0, 2);
	test(*r->s2, 0, 2, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s2, 0, 2, "abcdefghijklmnopqrst", 10, -8);
	test(*r->s2, 0, 2, "abcdefghijklmnopqrst", 19, -17);
	test(*r->s2, 0, 2, "abcdefghijklmnopqrst", 20, -18);
	test(*r->s2, 0, 4, "", 0, 4);
	test(*r->s2, 0, 4, "abcde", 0, 4);
	test(*r->s2, 0, 4, "abcde", 1, 3);
	test(*r->s2, 0, 4, "abcde", 2, 2);
}

template <class S>
void
test1(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s2, 0, 4, "abcde", 4, 0);
	test(*r->s2, 0, 4, "abcde", 5, -1);
	test(*r->s2, 0, 4, "abcdefghij", 0, 4);
	test(*r->s2, 0, 4, "abcdefghij", 1, 3);
	test(*r->s2, 0, 4, "abcdefghij", 5, -1);
	test(*r->s2, 0, 4, "abcdefghij", 9, -5);
	test(*r->s2, 0, 4, "abcdefghij", 10, -6);
	test(*r->s2, 0, 4, "abcdefghijklmnopqrst", 0, 4);
	test(*r->s2, 0, 4, "abcdefghijklmnopqrst", 1, 3);
	test(*r->s2, 0, 4, "abcdefghijklmnopqrst", 10, -6);
	test(*r->s2, 0, 4, "abcdefghijklmnopqrst", 19, -15);
	test(*r->s2, 0, 4, "abcdefghijklmnopqrst", 20, -16);
	test(*r->s2, 0, 5, "", 0, 5);
	test(*r->s2, 0, 5, "abcde", 0, 5);
	test(*r->s2, 0, 5, "abcde", 1, 4);
	test(*r->s2, 0, 5, "abcde", 2, 3);
	test(*r->s2, 0, 5, "abcde", 4, 1);
	test(*r->s2, 0, 5, "abcde", 5, 0);
	test(*r->s2, 0, 5, "abcdefghij", 0, 5);
	test(*r->s2, 0, 5, "abcdefghij", 1, 4);
	test(*r->s2, 0, 5, "abcdefghij", 5, 0);
	test(*r->s2, 0, 5, "abcdefghij", 9, -4);
	test(*r->s2, 0, 5, "abcdefghij", 10, -5);
	test(*r->s2, 0, 5, "abcdefghijklmnopqrst", 0, 5);
	test(*r->s2, 0, 5, "abcdefghijklmnopqrst", 1, 4);
	test(*r->s2, 0, 5, "abcdefghijklmnopqrst", 10, -5);
	test(*r->s2, 0, 5, "abcdefghijklmnopqrst", 19, -14);
	test(*r->s2, 0, 5, "abcdefghijklmnopqrst", 20, -15);
	test(*r->s2, 0, 6, "", 0, 5);
	test(*r->s2, 0, 6, "abcde", 0, 5);
	test(*r->s2, 0, 6, "abcde", 1, 4);
	test(*r->s2, 0, 6, "abcde", 2, 3);
	test(*r->s2, 0, 6, "abcde", 4, 1);
	test(*r->s2, 0, 6, "abcde", 5, 0);
	test(*r->s2, 0, 6, "abcdefghij", 0, 5);
	test(*r->s2, 0, 6, "abcdefghij", 1, 4);
	test(*r->s2, 0, 6, "abcdefghij", 5, 0);
	test(*r->s2, 0, 6, "abcdefghij", 9, -4);
	test(*r->s2, 0, 6, "abcdefghij", 10, -5);
	test(*r->s2, 0, 6, "abcdefghijklmnopqrst", 0, 5);
	test(*r->s2, 0, 6, "abcdefghijklmnopqrst", 1, 4);
	test(*r->s2, 0, 6, "abcdefghijklmnopqrst", 10, -5);
	test(*r->s2, 0, 6, "abcdefghijklmnopqrst", 19, -14);
	test(*r->s2, 0, 6, "abcdefghijklmnopqrst", 20, -15);
	test(*r->s2, 1, 0, "", 0, 0);
	test(*r->s2, 1, 0, "abcde", 0, 0);
	test(*r->s2, 1, 0, "abcde", 1, -1);
	test(*r->s2, 1, 0, "abcde", 2, -2);
	test(*r->s2, 1, 0, "abcde", 4, -4);
	test(*r->s2, 1, 0, "abcde", 5, -5);
	test(*r->s2, 1, 0, "abcdefghij", 0, 0);
	test(*r->s2, 1, 0, "abcdefghij", 1, -1);
	test(*r->s2, 1, 0, "abcdefghij", 5, -5);
	test(*r->s2, 1, 0, "abcdefghij", 9, -9);
	test(*r->s2, 1, 0, "abcdefghij", 10, -10);
	test(*r->s2, 1, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s2, 1, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s2, 1, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s2, 1, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s2, 1, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s2, 1, 1, "", 0, 1);
	test(*r->s2, 1, 1, "abcde", 0, 1);
	test(*r->s2, 1, 1, "abcde", 1, 1);
	test(*r->s2, 1, 1, "abcde", 2, 1);
	test(*r->s2, 1, 1, "abcde", 4, 1);
	test(*r->s2, 1, 1, "abcde", 5, 1);
	test(*r->s2, 1, 1, "abcdefghij", 0, 1);
	test(*r->s2, 1, 1, "abcdefghij", 1, 1);
	test(*r->s2, 1, 1, "abcdefghij", 5, 1);
	test(*r->s2, 1, 1, "abcdefghij", 9, 1);
	test(*r->s2, 1, 1, "abcdefghij", 10, 1);
	test(*r->s2, 1, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s2, 1, 1, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s2, 1, 1, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s2, 1, 1, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s2, 1, 1, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s2, 1, 2, "", 0, 2);
	test(*r->s2, 1, 2, "abcde", 0, 2);
	test(*r->s2, 1, 2, "abcde", 1, 1);
	test(*r->s2, 1, 2, "abcde", 2, 1);
	test(*r->s2, 1, 2, "abcde", 4, 1);
	test(*r->s2, 1, 2, "abcde", 5, 1);
	test(*r->s2, 1, 2, "abcdefghij", 0, 2);
	test(*r->s2, 1, 2, "abcdefghij", 1, 1);
	test(*r->s2, 1, 2, "abcdefghij", 5, 1);
	test(*r->s2, 1, 2, "abcdefghij", 9, 1);
	test(*r->s2, 1, 2, "abcdefghij", 10, 1);
	test(*r->s2, 1, 2, "abcdefghijklmnopqrst", 0, 2);
	test(*r->s2, 1, 2, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s2, 1, 2, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s2, 1, 2, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s2, 1, 2, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s2, 1, 3, "", 0, 3);
	test(*r->s2, 1, 3, "abcde", 0, 3);
	test(*r->s2, 1, 3, "abcde", 1, 1);
	test(*r->s2, 1, 3, "abcde", 2, 1);
	test(*r->s2, 1, 3, "abcde", 4, 1);
	test(*r->s2, 1, 3, "abcde", 5, 1);
	test(*r->s2, 1, 3, "abcdefghij", 0, 3);
	test(*r->s2, 1, 3, "abcdefghij", 1, 1);
}

template <class S>
void
test2(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s2, 1, 3, "abcdefghij", 5, 1);
	test(*r->s2, 1, 3, "abcdefghij", 9, 1);
	test(*r->s2, 1, 3, "abcdefghij", 10, 1);
	test(*r->s2, 1, 3, "abcdefghijklmnopqrst", 0, 3);
	test(*r->s2, 1, 3, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s2, 1, 3, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s2, 1, 3, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s2, 1, 3, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s2, 1, 4, "", 0, 4);
	test(*r->s2, 1, 4, "abcde", 0, 4);
	test(*r->s2, 1, 4, "abcde", 1, 1);
	test(*r->s2, 1, 4, "abcde", 2, 1);
	test(*r->s2, 1, 4, "abcde", 4, 1);
	test(*r->s2, 1, 4, "abcde", 5, 1);
	test(*r->s2, 1, 4, "abcdefghij", 0, 4);
	test(*r->s2, 1, 4, "abcdefghij", 1, 1);
	test(*r->s2, 1, 4, "abcdefghij", 5, 1);
	test(*r->s2, 1, 4, "abcdefghij", 9, 1);
	test(*r->s2, 1, 4, "abcdefghij", 10, 1);
	test(*r->s2, 1, 4, "abcdefghijklmnopqrst", 0, 4);
	test(*r->s2, 1, 4, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s2, 1, 4, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s2, 1, 4, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s2, 1, 4, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s2, 1, 5, "", 0, 4);
	test(*r->s2, 1, 5, "abcde", 0, 4);
	test(*r->s2, 1, 5, "abcde", 1, 1);
	test(*r->s2, 1, 5, "abcde", 2, 1);
	test(*r->s2, 1, 5, "abcde", 4, 1);
	test(*r->s2, 1, 5, "abcde", 5, 1);
	test(*r->s2, 1, 5, "abcdefghij", 0, 4);
	test(*r->s2, 1, 5, "abcdefghij", 1, 1);
	test(*r->s2, 1, 5, "abcdefghij", 5, 1);
	test(*r->s2, 1, 5, "abcdefghij", 9, 1);
	test(*r->s2, 1, 5, "abcdefghij", 10, 1);
	test(*r->s2, 1, 5, "abcdefghijklmnopqrst", 0, 4);
	test(*r->s2, 1, 5, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s2, 1, 5, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s2, 1, 5, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s2, 1, 5, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s2, 2, 0, "", 0, 0);
	test(*r->s2, 2, 0, "abcde", 0, 0);
	test(*r->s2, 2, 0, "abcde", 1, -1);
	test(*r->s2, 2, 0, "abcde", 2, -2);
	test(*r->s2, 2, 0, "abcde", 4, -4);
	test(*r->s2, 2, 0, "abcde", 5, -5);
	test(*r->s2, 2, 0, "abcdefghij", 0, 0);
	test(*r->s2, 2, 0, "abcdefghij", 1, -1);
	test(*r->s2, 2, 0, "abcdefghij", 5, -5);
	test(*r->s2, 2, 0, "abcdefghij", 9, -9);
	test(*r->s2, 2, 0, "abcdefghij", 10, -10);
	test(*r->s2, 2, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s2, 2, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s2, 2, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s2, 2, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s2, 2, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s2, 2, 1, "", 0, 1);
	test(*r->s2, 2, 1, "abcde", 0, 1);
	test(*r->s2, 2, 1, "abcde", 1, 2);
	test(*r->s2, 2, 1, "abcde", 2, 2);
	test(*r->s2, 2, 1, "abcde", 4, 2);
	test(*r->s2, 2, 1, "abcde", 5, 2);
	test(*r->s2, 2, 1, "abcdefghij", 0, 1);
	test(*r->s2, 2, 1, "abcdefghij", 1, 2);
	test(*r->s2, 2, 1, "abcdefghij", 5, 2);
	test(*r->s2, 2, 1, "abcdefghij", 9, 2);
	test(*r->s2, 2, 1, "abcdefghij", 10, 2);
	test(*r->s2, 2, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s2, 2, 1, "abcdefghijklmnopqrst", 1, 2);
	test(*r->s2, 2, 1, "abcdefghijklmnopqrst", 10, 2);
	test(*r->s2, 2, 1, "abcdefghijklmnopqrst", 19, 2);
	test(*r->s2, 2, 1, "abcdefghijklmnopqrst", 20, 2);
	test(*r->s2, 2, 2, "", 0, 2);
	test(*r->s2, 2, 2, "abcde", 0, 2);
	test(*r->s2, 2, 2, "abcde", 1, 2);
	test(*r->s2, 2, 2, "abcde", 2, 2);
	test(*r->s2, 2, 2, "abcde", 4, 2);
	test(*r->s2, 2, 2, "abcde", 5, 2);
	test(*r->s2, 2, 2, "abcdefghij", 0, 2);
	test(*r->s2, 2, 2, "abcdefghij", 1, 2);
	test(*r->s2, 2, 2, "abcdefghij", 5, 2);
	test(*r->s2, 2, 2, "abcdefghij", 9, 2);
	test(*r->s2, 2, 2, "abcdefghij", 10, 2);
	test(*r->s2, 2, 2, "abcdefghijklmnopqrst", 0, 2);
	test(*r->s2, 2, 2, "abcdefghijklmnopqrst", 1, 2);
	test(*r->s2, 2, 2, "abcdefghijklmnopqrst", 10, 2);
	test(*r->s2, 2, 2, "abcdefghijklmnopqrst", 19, 2);
	test(*r->s2, 2, 2, "abcdefghijklmnopqrst", 20, 2);
	test(*r->s2, 2, 3, "", 0, 3);
	test(*r->s2, 2, 3, "abcde", 0, 3);
	test(*r->s2, 2, 3, "abcde", 1, 2);
	test(*r->s2, 2, 3, "abcde", 2, 2);
	test(*r->s2, 2, 3, "abcde", 4, 2);
	test(*r->s2, 2, 3, "abcde", 5, 2);
	test(*r->s2, 2, 3, "abcdefghij", 0, 3);
	test(*r->s2, 2, 3, "abcdefghij", 1, 2);
	test(*r->s2, 2, 3, "abcdefghij", 5, 2);
	test(*r->s2, 2, 3, "abcdefghij", 9, 2);
	test(*r->s2, 2, 3, "abcdefghij", 10, 2);
	test(*r->s2, 2, 3, "abcdefghijklmnopqrst", 0, 3);
}

template <class S>
void
test3(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s2, 2, 3, "abcdefghijklmnopqrst", 1, 2);
	test(*r->s2, 2, 3, "abcdefghijklmnopqrst", 10, 2);
	test(*r->s2, 2, 3, "abcdefghijklmnopqrst", 19, 2);
	test(*r->s2, 2, 3, "abcdefghijklmnopqrst", 20, 2);
	test(*r->s2, 2, 4, "", 0, 3);
	test(*r->s2, 2, 4, "abcde", 0, 3);
	test(*r->s2, 2, 4, "abcde", 1, 2);
	test(*r->s2, 2, 4, "abcde", 2, 2);
	test(*r->s2, 2, 4, "abcde", 4, 2);
	test(*r->s2, 2, 4, "abcde", 5, 2);
	test(*r->s2, 2, 4, "abcdefghij", 0, 3);
	test(*r->s2, 2, 4, "abcdefghij", 1, 2);
	test(*r->s2, 2, 4, "abcdefghij", 5, 2);
	test(*r->s2, 2, 4, "abcdefghij", 9, 2);
	test(*r->s2, 2, 4, "abcdefghij", 10, 2);
	test(*r->s2, 2, 4, "abcdefghijklmnopqrst", 0, 3);
	test(*r->s2, 2, 4, "abcdefghijklmnopqrst", 1, 2);
	test(*r->s2, 2, 4, "abcdefghijklmnopqrst", 10, 2);
	test(*r->s2, 2, 4, "abcdefghijklmnopqrst", 19, 2);
	test(*r->s2, 2, 4, "abcdefghijklmnopqrst", 20, 2);
	test(*r->s2, 4, 0, "", 0, 0);
	test(*r->s2, 4, 0, "abcde", 0, 0);
	test(*r->s2, 4, 0, "abcde", 1, -1);
	test(*r->s2, 4, 0, "abcde", 2, -2);
	test(*r->s2, 4, 0, "abcde", 4, -4);
	test(*r->s2, 4, 0, "abcde", 5, -5);
	test(*r->s2, 4, 0, "abcdefghij", 0, 0);
	test(*r->s2, 4, 0, "abcdefghij", 1, -1);
	test(*r->s2, 4, 0, "abcdefghij", 5, -5);
	test(*r->s2, 4, 0, "abcdefghij", 9, -9);
	test(*r->s2, 4, 0, "abcdefghij", 10, -10);
	test(*r->s2, 4, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s2, 4, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s2, 4, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s2, 4, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s2, 4, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s2, 4, 1, "", 0, 1);
	test(*r->s2, 4, 1, "abcde", 0, 1);
	test(*r->s2, 4, 1, "abcde", 1, 4);
	test(*r->s2, 4, 1, "abcde", 2, 4);
	test(*r->s2, 4, 1, "abcde", 4, 4);
	test(*r->s2, 4, 1, "abcde", 5, 4);
	test(*r->s2, 4, 1, "abcdefghij", 0, 1);
	test(*r->s2, 4, 1, "abcdefghij", 1, 4);
	test(*r->s2, 4, 1, "abcdefghij", 5, 4);
	test(*r->s2, 4, 1, "abcdefghij", 9, 4);
	test(*r->s2, 4, 1, "abcdefghij", 10, 4);
	test(*r->s2, 4, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s2, 4, 1, "abcdefghijklmnopqrst", 1, 4);
	test(*r->s2, 4, 1, "abcdefghijklmnopqrst", 10, 4);
	test(*r->s2, 4, 1, "abcdefghijklmnopqrst", 19, 4);
	test(*r->s2, 4, 1, "abcdefghijklmnopqrst", 20, 4);
	test(*r->s2, 4, 2, "", 0, 1);
	test(*r->s2, 4, 2, "abcde", 0, 1);
	test(*r->s2, 4, 2, "abcde", 1, 4);
	test(*r->s2, 4, 2, "abcde", 2, 4);
	test(*r->s2, 4, 2, "abcde", 4, 4);
	test(*r->s2, 4, 2, "abcde", 5, 4);
	test(*r->s2, 4, 2, "abcdefghij", 0, 1);
	test(*r->s2, 4, 2, "abcdefghij", 1, 4);
	test(*r->s2, 4, 2, "abcdefghij", 5, 4);
	test(*r->s2, 4, 2, "abcdefghij", 9, 4);
	test(*r->s2, 4, 2, "abcdefghij", 10, 4);
	test(*r->s2, 4, 2, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s2, 4, 2, "abcdefghijklmnopqrst", 1, 4);
	test(*r->s2, 4, 2, "abcdefghijklmnopqrst", 10, 4);
	test(*r->s2, 4, 2, "abcdefghijklmnopqrst", 19, 4);
	test(*r->s2, 4, 2, "abcdefghijklmnopqrst", 20, 4);
	test(*r->s2, 5, 0, "", 0, 0);
	test(*r->s2, 5, 0, "abcde", 0, 0);
	test(*r->s2, 5, 0, "abcde", 1, -1);
	test(*r->s2, 5, 0, "abcde", 2, -2);
	test(*r->s2, 5, 0, "abcde", 4, -4);
	test(*r->s2, 5, 0, "abcde", 5, -5);
	test(*r->s2, 5, 0, "abcdefghij", 0, 0);
	test(*r->s2, 5, 0, "abcdefghij", 1, -1);
	test(*r->s2, 5, 0, "abcdefghij", 5, -5);
	test(*r->s2, 5, 0, "abcdefghij", 9, -9);
	test(*r->s2, 5, 0, "abcdefghij", 10, -10);
	test(*r->s2, 5, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s2, 5, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s2, 5, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s2, 5, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s2, 5, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s2, 5, 1, "", 0, 0);
	test(*r->s2, 5, 1, "abcde", 0, 0);
	test(*r->s2, 5, 1, "abcde", 1, -1);
	test(*r->s2, 5, 1, "abcde", 2, -2);
	test(*r->s2, 5, 1, "abcde", 4, -4);
	test(*r->s2, 5, 1, "abcde", 5, -5);
	test(*r->s2, 5, 1, "abcdefghij", 0, 0);
	test(*r->s2, 5, 1, "abcdefghij", 1, -1);
	test(*r->s2, 5, 1, "abcdefghij", 5, -5);
	test(*r->s2, 5, 1, "abcdefghij", 9, -9);
	test(*r->s2, 5, 1, "abcdefghij", 10, -10);
	test(*r->s2, 5, 1, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s2, 5, 1, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s2, 5, 1, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s2, 5, 1, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s2, 5, 1, "abcdefghijklmnopqrst", 20, -20);
}

template <class S>
void
test4(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s2, 6, 0, "", 0, 0);
	test(*r->s2, 6, 0, "abcde", 0, 0);
	test(*r->s2, 6, 0, "abcde", 1, 0);
	test(*r->s2, 6, 0, "abcde", 2, 0);
	test(*r->s2, 6, 0, "abcde", 4, 0);
	test(*r->s2, 6, 0, "abcde", 5, 0);
	test(*r->s2, 6, 0, "abcdefghij", 0, 0);
	test(*r->s2, 6, 0, "abcdefghij", 1, 0);
	test(*r->s2, 6, 0, "abcdefghij", 5, 0);
	test(*r->s2, 6, 0, "abcdefghij", 9, 0);
	test(*r->s2, 6, 0, "abcdefghij", 10, 0);
	test(*r->s2, 6, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s2, 6, 0, "abcdefghijklmnopqrst", 1, 0);
	test(*r->s2, 6, 0, "abcdefghijklmnopqrst", 10, 0);
	test(*r->s2, 6, 0, "abcdefghijklmnopqrst", 19, 0);
	test(*r->s2, 6, 0, "abcdefghijklmnopqrst", 20, 0);
	test(*r->s3, 0, 0, "", 0, 0);
	test(*r->s3, 0, 0, "abcde", 0, 0);
	test(*r->s3, 0, 0, "abcde", 1, -1);
	test(*r->s3, 0, 0, "abcde", 2, -2);
	test(*r->s3, 0, 0, "abcde", 4, -4);
	test(*r->s3, 0, 0, "abcde", 5, -5);
	test(*r->s3, 0, 0, "abcdefghij", 0, 0);
	test(*r->s3, 0, 0, "abcdefghij", 1, -1);
	test(*r->s3, 0, 0, "abcdefghij", 5, -5);
	test(*r->s3, 0, 0, "abcdefghij", 9, -9);
	test(*r->s3, 0, 0, "abcdefghij", 10, -10);
	test(*r->s3, 0, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s3, 0, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s3, 0, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s3, 0, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s3, 0, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s3, 0, 1, "", 0, 1);
	test(*r->s3, 0, 1, "abcde", 0, 1);
	test(*r->s3, 0, 1, "abcde", 1, 0);
	test(*r->s3, 0, 1, "abcde", 2, -1);
	test(*r->s3, 0, 1, "abcde", 4, -3);
	test(*r->s3, 0, 1, "abcde", 5, -4);
	test(*r->s3, 0, 1, "abcdefghij", 0, 1);
	test(*r->s3, 0, 1, "abcdefghij", 1, 0);
	test(*r->s3, 0, 1, "abcdefghij", 5, -4);
	test(*r->s3, 0, 1, "abcdefghij", 9, -8);
	test(*r->s3, 0, 1, "abcdefghij", 10, -9);
	test(*r->s3, 0, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s3, 0, 1, "abcdefghijklmnopqrst", 1, 0);
	test(*r->s3, 0, 1, "abcdefghijklmnopqrst", 10, -9);
	test(*r->s3, 0, 1, "abcdefghijklmnopqrst", 19, -18);
	test(*r->s3, 0, 1, "abcdefghijklmnopqrst", 20, -19);
	test(*r->s3, 0, 5, "", 0, 5);
	test(*r->s3, 0, 5, "abcde", 0, 5);
	test(*r->s3, 0, 5, "abcde", 1, 4);
	test(*r->s3, 0, 5, "abcde", 2, 3);
	test(*r->s3, 0, 5, "abcde", 4, 1);
	test(*r->s3, 0, 5, "abcde", 5, 0);
	test(*r->s3, 0, 5, "abcdefghij", 0, 5);
	test(*r->s3, 0, 5, "abcdefghij", 1, 4);
	test(*r->s3, 0, 5, "abcdefghij", 5, 0);
	test(*r->s3, 0, 5, "abcdefghij", 9, -4);
	test(*r->s3, 0, 5, "abcdefghij", 10, -5);
	test(*r->s3, 0, 5, "abcdefghijklmnopqrst", 0, 5);
	test(*r->s3, 0, 5, "abcdefghijklmnopqrst", 1, 4);
	test(*r->s3, 0, 5, "abcdefghijklmnopqrst", 10, -5);
	test(*r->s3, 0, 5, "abcdefghijklmnopqrst", 19, -14);
	test(*r->s3, 0, 5, "abcdefghijklmnopqrst", 20, -15);
	test(*r->s3, 0, 9, "", 0, 9);
	test(*r->s3, 0, 9, "abcde", 0, 9);
	test(*r->s3, 0, 9, "abcde", 1, 8);
	test(*r->s3, 0, 9, "abcde", 2, 7);
	test(*r->s3, 0, 9, "abcde", 4, 5);
	test(*r->s3, 0, 9, "abcde", 5, 4);
	test(*r->s3, 0, 9, "abcdefghij", 0, 9);
	test(*r->s3, 0, 9, "abcdefghij", 1, 8);
	test(*r->s3, 0, 9, "abcdefghij", 5, 4);
	test(*r->s3, 0, 9, "abcdefghij", 9, 0);
	test(*r->s3, 0, 9, "abcdefghij", 10, -1);
	test(*r->s3, 0, 9, "abcdefghijklmnopqrst", 0, 9);
	test(*r->s3, 0, 9, "abcdefghijklmnopqrst", 1, 8);
	test(*r->s3, 0, 9, "abcdefghijklmnopqrst", 10, -1);
	test(*r->s3, 0, 9, "abcdefghijklmnopqrst", 19, -10);
	test(*r->s3, 0, 9, "abcdefghijklmnopqrst", 20, -11);
	test(*r->s3, 0, 10, "", 0, 10);
	test(*r->s3, 0, 10, "abcde", 0, 10);
	test(*r->s3, 0, 10, "abcde", 1, 9);
	test(*r->s3, 0, 10, "abcde", 2, 8);
	test(*r->s3, 0, 10, "abcde", 4, 6);
	test(*r->s3, 0, 10, "abcde", 5, 5);
	test(*r->s3, 0, 10, "abcdefghij", 0, 10);
	test(*r->s3, 0, 10, "abcdefghij", 1, 9);
	test(*r->s3, 0, 10, "abcdefghij", 5, 5);
	test(*r->s3, 0, 10, "abcdefghij", 9, 1);
	test(*r->s3, 0, 10, "abcdefghij", 10, 0);
	test(*r->s3, 0, 10, "abcdefghijklmnopqrst", 0, 10);
	test(*r->s3, 0, 10, "abcdefghijklmnopqrst", 1, 9);
	test(*r->s3, 0, 10, "abcdefghijklmnopqrst", 10, 0);
	test(*r->s3, 0, 10, "abcdefghijklmnopqrst", 19, -9);
	test(*r->s3, 0, 10, "abcdefghijklmnopqrst", 20, -10);
	test(*r->s3, 0, 11, "", 0, 10);
	test(*r->s3, 0, 11, "abcde", 0, 10);
	test(*r->s3, 0, 11, "abcde", 1, 9);
	test(*r->s3, 0, 11, "abcde", 2, 8);
}

template <class S>
void
test5(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s3, 0, 11, "abcde", 4, 6);
	test(*r->s3, 0, 11, "abcde", 5, 5);
	test(*r->s3, 0, 11, "abcdefghij", 0, 10);
	test(*r->s3, 0, 11, "abcdefghij", 1, 9);
	test(*r->s3, 0, 11, "abcdefghij", 5, 5);
	test(*r->s3, 0, 11, "abcdefghij", 9, 1);
	test(*r->s3, 0, 11, "abcdefghij", 10, 0);
	test(*r->s3, 0, 11, "abcdefghijklmnopqrst", 0, 10);
	test(*r->s3, 0, 11, "abcdefghijklmnopqrst", 1, 9);
	test(*r->s3, 0, 11, "abcdefghijklmnopqrst", 10, 0);
	test(*r->s3, 0, 11, "abcdefghijklmnopqrst", 19, -9);
	test(*r->s3, 0, 11, "abcdefghijklmnopqrst", 20, -10);
	test(*r->s3, 1, 0, "", 0, 0);
	test(*r->s3, 1, 0, "abcde", 0, 0);
	test(*r->s3, 1, 0, "abcde", 1, -1);
	test(*r->s3, 1, 0, "abcde", 2, -2);
	test(*r->s3, 1, 0, "abcde", 4, -4);
	test(*r->s3, 1, 0, "abcde", 5, -5);
	test(*r->s3, 1, 0, "abcdefghij", 0, 0);
	test(*r->s3, 1, 0, "abcdefghij", 1, -1);
	test(*r->s3, 1, 0, "abcdefghij", 5, -5);
	test(*r->s3, 1, 0, "abcdefghij", 9, -9);
	test(*r->s3, 1, 0, "abcdefghij", 10, -10);
	test(*r->s3, 1, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s3, 1, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s3, 1, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s3, 1, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s3, 1, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s3, 1, 1, "", 0, 1);
	test(*r->s3, 1, 1, "abcde", 0, 1);
	test(*r->s3, 1, 1, "abcde", 1, 1);
	test(*r->s3, 1, 1, "abcde", 2, 1);
	test(*r->s3, 1, 1, "abcde", 4, 1);
	test(*r->s3, 1, 1, "abcde", 5, 1);
	test(*r->s3, 1, 1, "abcdefghij", 0, 1);
	test(*r->s3, 1, 1, "abcdefghij", 1, 1);
	test(*r->s3, 1, 1, "abcdefghij", 5, 1);
	test(*r->s3, 1, 1, "abcdefghij", 9, 1);
	test(*r->s3, 1, 1, "abcdefghij", 10, 1);
	test(*r->s3, 1, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s3, 1, 1, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s3, 1, 1, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s3, 1, 1, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s3, 1, 1, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s3, 1, 4, "", 0, 4);
	test(*r->s3, 1, 4, "abcde", 0, 4);
	test(*r->s3, 1, 4, "abcde", 1, 1);
	test(*r->s3, 1, 4, "abcde", 2, 1);
	test(*r->s3, 1, 4, "abcde", 4, 1);
	test(*r->s3, 1, 4, "abcde", 5, 1);
	test(*r->s3, 1, 4, "abcdefghij", 0, 4);
	test(*r->s3, 1, 4, "abcdefghij", 1, 1);
	test(*r->s3, 1, 4, "abcdefghij", 5, 1);
	test(*r->s3, 1, 4, "abcdefghij", 9, 1);
	test(*r->s3, 1, 4, "abcdefghij", 10, 1);
	test(*r->s3, 1, 4, "abcdefghijklmnopqrst", 0, 4);
	test(*r->s3, 1, 4, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s3, 1, 4, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s3, 1, 4, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s3, 1, 4, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s3, 1, 8, "", 0, 8);
	test(*r->s3, 1, 8, "abcde", 0, 8);
	test(*r->s3, 1, 8, "abcde", 1, 1);
	test(*r->s3, 1, 8, "abcde", 2, 1);
	test(*r->s3, 1, 8, "abcde", 4, 1);
	test(*r->s3, 1, 8, "abcde", 5, 1);
	test(*r->s3, 1, 8, "abcdefghij", 0, 8);
	test(*r->s3, 1, 8, "abcdefghij", 1, 1);
	test(*r->s3, 1, 8, "abcdefghij", 5, 1);
	test(*r->s3, 1, 8, "abcdefghij", 9, 1);
	test(*r->s3, 1, 8, "abcdefghij", 10, 1);
	test(*r->s3, 1, 8, "abcdefghijklmnopqrst", 0, 8);
	test(*r->s3, 1, 8, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s3, 1, 8, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s3, 1, 8, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s3, 1, 8, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s3, 1, 9, "", 0, 9);
	test(*r->s3, 1, 9, "abcde", 0, 9);
	test(*r->s3, 1, 9, "abcde", 1, 1);
	test(*r->s3, 1, 9, "abcde", 2, 1);
	test(*r->s3, 1, 9, "abcde", 4, 1);
	test(*r->s3, 1, 9, "abcde", 5, 1);
	test(*r->s3, 1, 9, "abcdefghij", 0, 9);
	test(*r->s3, 1, 9, "abcdefghij", 1, 1);
	test(*r->s3, 1, 9, "abcdefghij", 5, 1);
	test(*r->s3, 1, 9, "abcdefghij", 9, 1);
	test(*r->s3, 1, 9, "abcdefghij", 10, 1);
	test(*r->s3, 1, 9, "abcdefghijklmnopqrst", 0, 9);
	test(*r->s3, 1, 9, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s3, 1, 9, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s3, 1, 9, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s3, 1, 9, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s3, 1, 10, "", 0, 9);
	test(*r->s3, 1, 10, "abcde", 0, 9);
	test(*r->s3, 1, 10, "abcde", 1, 1);
	test(*r->s3, 1, 10, "abcde", 2, 1);
	test(*r->s3, 1, 10, "abcde", 4, 1);
	test(*r->s3, 1, 10, "abcde", 5, 1);
	test(*r->s3, 1, 10, "abcdefghij", 0, 9);
	test(*r->s3, 1, 10, "abcdefghij", 1, 1);
}

template <class S>
void
test6(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s3, 1, 10, "abcdefghij", 5, 1);
	test(*r->s3, 1, 10, "abcdefghij", 9, 1);
	test(*r->s3, 1, 10, "abcdefghij", 10, 1);
	test(*r->s3, 1, 10, "abcdefghijklmnopqrst", 0, 9);
	test(*r->s3, 1, 10, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s3, 1, 10, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s3, 1, 10, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s3, 1, 10, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s3, 5, 0, "", 0, 0);
	test(*r->s3, 5, 0, "abcde", 0, 0);
	test(*r->s3, 5, 0, "abcde", 1, -1);
	test(*r->s3, 5, 0, "abcde", 2, -2);
	test(*r->s3, 5, 0, "abcde", 4, -4);
	test(*r->s3, 5, 0, "abcde", 5, -5);
	test(*r->s3, 5, 0, "abcdefghij", 0, 0);
	test(*r->s3, 5, 0, "abcdefghij", 1, -1);
	test(*r->s3, 5, 0, "abcdefghij", 5, -5);
	test(*r->s3, 5, 0, "abcdefghij", 9, -9);
	test(*r->s3, 5, 0, "abcdefghij", 10, -10);
	test(*r->s3, 5, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s3, 5, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s3, 5, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s3, 5, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s3, 5, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s3, 5, 1, "", 0, 1);
	test(*r->s3, 5, 1, "abcde", 0, 1);
	test(*r->s3, 5, 1, "abcde", 1, 5);
	test(*r->s3, 5, 1, "abcde", 2, 5);
	test(*r->s3, 5, 1, "abcde", 4, 5);
	test(*r->s3, 5, 1, "abcde", 5, 5);
	test(*r->s3, 5, 1, "abcdefghij", 0, 1);
	test(*r->s3, 5, 1, "abcdefghij", 1, 5);
	test(*r->s3, 5, 1, "abcdefghij", 5, 5);
	test(*r->s3, 5, 1, "abcdefghij", 9, 5);
	test(*r->s3, 5, 1, "abcdefghij", 10, 5);
	test(*r->s3, 5, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s3, 5, 1, "abcdefghijklmnopqrst", 1, 5);
	test(*r->s3, 5, 1, "abcdefghijklmnopqrst", 10, 5);
	test(*r->s3, 5, 1, "abcdefghijklmnopqrst", 19, 5);
	test(*r->s3, 5, 1, "abcdefghijklmnopqrst", 20, 5);
	test(*r->s3, 5, 2, "", 0, 2);
	test(*r->s3, 5, 2, "abcde", 0, 2);
	test(*r->s3, 5, 2, "abcde", 1, 5);
	test(*r->s3, 5, 2, "abcde", 2, 5);
	test(*r->s3, 5, 2, "abcde", 4, 5);
	test(*r->s3, 5, 2, "abcde", 5, 5);
	test(*r->s3, 5, 2, "abcdefghij", 0, 2);
	test(*r->s3, 5, 2, "abcdefghij", 1, 5);
	test(*r->s3, 5, 2, "abcdefghij", 5, 5);
	test(*r->s3, 5, 2, "abcdefghij", 9, 5);
	test(*r->s3, 5, 2, "abcdefghij", 10, 5);
	test(*r->s3, 5, 2, "abcdefghijklmnopqrst", 0, 2);
	test(*r->s3, 5, 2, "abcdefghijklmnopqrst", 1, 5);
	test(*r->s3, 5, 2, "abcdefghijklmnopqrst", 10, 5);
	test(*r->s3, 5, 2, "abcdefghijklmnopqrst", 19, 5);
	test(*r->s3, 5, 2, "abcdefghijklmnopqrst", 20, 5);
	test(*r->s3, 5, 4, "", 0, 4);
	test(*r->s3, 5, 4, "abcde", 0, 4);
	test(*r->s3, 5, 4, "abcde", 1, 5);
	test(*r->s3, 5, 4, "abcde", 2, 5);
	test(*r->s3, 5, 4, "abcde", 4, 5);
	test(*r->s3, 5, 4, "abcde", 5, 5);
	test(*r->s3, 5, 4, "abcdefghij", 0, 4);
	test(*r->s3, 5, 4, "abcdefghij", 1, 5);
	test(*r->s3, 5, 4, "abcdefghij", 5, 5);
	test(*r->s3, 5, 4, "abcdefghij", 9, 5);
	test(*r->s3, 5, 4, "abcdefghij", 10, 5);
	test(*r->s3, 5, 4, "abcdefghijklmnopqrst", 0, 4);
	test(*r->s3, 5, 4, "abcdefghijklmnopqrst", 1, 5);
	test(*r->s3, 5, 4, "abcdefghijklmnopqrst", 10, 5);
	test(*r->s3, 5, 4, "abcdefghijklmnopqrst", 19, 5);
	test(*r->s3, 5, 4, "abcdefghijklmnopqrst", 20, 5);
	test(*r->s3, 5, 5, "", 0, 5);
	test(*r->s3, 5, 5, "abcde", 0, 5);
	test(*r->s3, 5, 5, "abcde", 1, 5);
	test(*r->s3, 5, 5, "abcde", 2, 5);
	test(*r->s3, 5, 5, "abcde", 4, 5);
	test(*r->s3, 5, 5, "abcde", 5, 5);
	test(*r->s3, 5, 5, "abcdefghij", 0, 5);
	test(*r->s3, 5, 5, "abcdefghij", 1, 5);
	test(*r->s3, 5, 5, "abcdefghij", 5, 5);
	test(*r->s3, 5, 5, "abcdefghij", 9, 5);
	test(*r->s3, 5, 5, "abcdefghij", 10, 5);
	test(*r->s3, 5, 5, "abcdefghijklmnopqrst", 0, 5);
	test(*r->s3, 5, 5, "abcdefghijklmnopqrst", 1, 5);
	test(*r->s3, 5, 5, "abcdefghijklmnopqrst", 10, 5);
	test(*r->s3, 5, 5, "abcdefghijklmnopqrst", 19, 5);
	test(*r->s3, 5, 5, "abcdefghijklmnopqrst", 20, 5);
	test(*r->s3, 5, 6, "", 0, 5);
	test(*r->s3, 5, 6, "abcde", 0, 5);
	test(*r->s3, 5, 6, "abcde", 1, 5);
	test(*r->s3, 5, 6, "abcde", 2, 5);
	test(*r->s3, 5, 6, "abcde", 4, 5);
	test(*r->s3, 5, 6, "abcde", 5, 5);
	test(*r->s3, 5, 6, "abcdefghij", 0, 5);
	test(*r->s3, 5, 6, "abcdefghij", 1, 5);
	test(*r->s3, 5, 6, "abcdefghij", 5, 5);
	test(*r->s3, 5, 6, "abcdefghij", 9, 5);
	test(*r->s3, 5, 6, "abcdefghij", 10, 5);
	test(*r->s3, 5, 6, "abcdefghijklmnopqrst", 0, 5);
}

template <class S>
void
test7(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s3, 5, 6, "abcdefghijklmnopqrst", 1, 5);
	test(*r->s3, 5, 6, "abcdefghijklmnopqrst", 10, 5);
	test(*r->s3, 5, 6, "abcdefghijklmnopqrst", 19, 5);
	test(*r->s3, 5, 6, "abcdefghijklmnopqrst", 20, 5);
	test(*r->s3, 9, 0, "", 0, 0);
	test(*r->s3, 9, 0, "abcde", 0, 0);
	test(*r->s3, 9, 0, "abcde", 1, -1);
	test(*r->s3, 9, 0, "abcde", 2, -2);
	test(*r->s3, 9, 0, "abcde", 4, -4);
	test(*r->s3, 9, 0, "abcde", 5, -5);
	test(*r->s3, 9, 0, "abcdefghij", 0, 0);
	test(*r->s3, 9, 0, "abcdefghij", 1, -1);
	test(*r->s3, 9, 0, "abcdefghij", 5, -5);
	test(*r->s3, 9, 0, "abcdefghij", 9, -9);
	test(*r->s3, 9, 0, "abcdefghij", 10, -10);
	test(*r->s3, 9, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s3, 9, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s3, 9, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s3, 9, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s3, 9, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s3, 9, 1, "", 0, 1);
	test(*r->s3, 9, 1, "abcde", 0, 1);
	test(*r->s3, 9, 1, "abcde", 1, 9);
	test(*r->s3, 9, 1, "abcde", 2, 9);
	test(*r->s3, 9, 1, "abcde", 4, 9);
	test(*r->s3, 9, 1, "abcde", 5, 9);
	test(*r->s3, 9, 1, "abcdefghij", 0, 1);
	test(*r->s3, 9, 1, "abcdefghij", 1, 9);
	test(*r->s3, 9, 1, "abcdefghij", 5, 9);
	test(*r->s3, 9, 1, "abcdefghij", 9, 9);
	test(*r->s3, 9, 1, "abcdefghij", 10, 9);
	test(*r->s3, 9, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s3, 9, 1, "abcdefghijklmnopqrst", 1, 9);
	test(*r->s3, 9, 1, "abcdefghijklmnopqrst", 10, 9);
	test(*r->s3, 9, 1, "abcdefghijklmnopqrst", 19, 9);
	test(*r->s3, 9, 1, "abcdefghijklmnopqrst", 20, 9);
	test(*r->s3, 9, 2, "", 0, 1);
	test(*r->s3, 9, 2, "abcde", 0, 1);
	test(*r->s3, 9, 2, "abcde", 1, 9);
	test(*r->s3, 9, 2, "abcde", 2, 9);
	test(*r->s3, 9, 2, "abcde", 4, 9);
	test(*r->s3, 9, 2, "abcde", 5, 9);
	test(*r->s3, 9, 2, "abcdefghij", 0, 1);
	test(*r->s3, 9, 2, "abcdefghij", 1, 9);
	test(*r->s3, 9, 2, "abcdefghij", 5, 9);
	test(*r->s3, 9, 2, "abcdefghij", 9, 9);
	test(*r->s3, 9, 2, "abcdefghij", 10, 9);
	test(*r->s3, 9, 2, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s3, 9, 2, "abcdefghijklmnopqrst", 1, 9);
	test(*r->s3, 9, 2, "abcdefghijklmnopqrst", 10, 9);
	test(*r->s3, 9, 2, "abcdefghijklmnopqrst", 19, 9);
	test(*r->s3, 9, 2, "abcdefghijklmnopqrst", 20, 9);
	test(*r->s3, 10, 0, "", 0, 0);
	test(*r->s3, 10, 0, "abcde", 0, 0);
	test(*r->s3, 10, 0, "abcde", 1, -1);
	test(*r->s3, 10, 0, "abcde", 2, -2);
	test(*r->s3, 10, 0, "abcde", 4, -4);
	test(*r->s3, 10, 0, "abcde", 5, -5);
	test(*r->s3, 10, 0, "abcdefghij", 0, 0);
	test(*r->s3, 10, 0, "abcdefghij", 1, -1);
	test(*r->s3, 10, 0, "abcdefghij", 5, -5);
	test(*r->s3, 10, 0, "abcdefghij", 9, -9);
	test(*r->s3, 10, 0, "abcdefghij", 10, -10);
	test(*r->s3, 10, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s3, 10, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s3, 10, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s3, 10, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s3, 10, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s3, 10, 1, "", 0, 0);
	test(*r->s3, 10, 1, "abcde", 0, 0);
	test(*r->s3, 10, 1, "abcde", 1, -1);
	test(*r->s3, 10, 1, "abcde", 2, -2);
	test(*r->s3, 10, 1, "abcde", 4, -4);
	test(*r->s3, 10, 1, "abcde", 5, -5);
	test(*r->s3, 10, 1, "abcdefghij", 0, 0);
	test(*r->s3, 10, 1, "abcdefghij", 1, -1);
	test(*r->s3, 10, 1, "abcdefghij", 5, -5);
	test(*r->s3, 10, 1, "abcdefghij", 9, -9);
	test(*r->s3, 10, 1, "abcdefghij", 10, -10);
	test(*r->s3, 10, 1, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s3, 10, 1, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s3, 10, 1, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s3, 10, 1, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s3, 10, 1, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s3, 11, 0, "", 0, 0);
	test(*r->s3, 11, 0, "abcde", 0, 0);
	test(*r->s3, 11, 0, "abcde", 1, 0);
	test(*r->s3, 11, 0, "abcde", 2, 0);
	test(*r->s3, 11, 0, "abcde", 4, 0);
	test(*r->s3, 11, 0, "abcde", 5, 0);
	test(*r->s3, 11, 0, "abcdefghij", 0, 0);
	test(*r->s3, 11, 0, "abcdefghij", 1, 0);
	test(*r->s3, 11, 0, "abcdefghij", 5, 0);
	test(*r->s3, 11, 0, "abcdefghij", 9, 0);
	test(*r->s3, 11, 0, "abcdefghij", 10, 0);
	test(*r->s3, 11, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s3, 11, 0, "abcdefghijklmnopqrst", 1, 0);
	test(*r->s3, 11, 0, "abcdefghijklmnopqrst", 10, 0);
	test(*r->s3, 11, 0, "abcdefghijklmnopqrst", 19, 0);
	test(*r->s3, 11, 0, "abcdefghijklmnopqrst", 20, 0);
}

template <class S>
void
test8(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s4, 0, 0, "", 0, 0);
	test(*r->s4, 0, 0, "abcde", 0, 0);
	test(*r->s4, 0, 0, "abcde", 1, -1);
	test(*r->s4, 0, 0, "abcde", 2, -2);
	test(*r->s4, 0, 0, "abcde", 4, -4);
	test(*r->s4, 0, 0, "abcde", 5, -5);
	test(*r->s4, 0, 0, "abcdefghij", 0, 0);
	test(*r->s4, 0, 0, "abcdefghij", 1, -1);
	test(*r->s4, 0, 0, "abcdefghij", 5, -5);
	test(*r->s4, 0, 0, "abcdefghij", 9, -9);
	test(*r->s4, 0, 0, "abcdefghij", 10, -10);
	test(*r->s4, 0, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s4, 0, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s4, 0, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s4, 0, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s4, 0, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s4, 0, 1, "", 0, 1);
	test(*r->s4, 0, 1, "abcde", 0, 1);
	test(*r->s4, 0, 1, "abcde", 1, 0);
	test(*r->s4, 0, 1, "abcde", 2, -1);
	test(*r->s4, 0, 1, "abcde", 4, -3);
	test(*r->s4, 0, 1, "abcde", 5, -4);
	test(*r->s4, 0, 1, "abcdefghij", 0, 1);
	test(*r->s4, 0, 1, "abcdefghij", 1, 0);
	test(*r->s4, 0, 1, "abcdefghij", 5, -4);
	test(*r->s4, 0, 1, "abcdefghij", 9, -8);
	test(*r->s4, 0, 1, "abcdefghij", 10, -9);
	test(*r->s4, 0, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s4, 0, 1, "abcdefghijklmnopqrst", 1, 0);
	test(*r->s4, 0, 1, "abcdefghijklmnopqrst", 10, -9);
	test(*r->s4, 0, 1, "abcdefghijklmnopqrst", 19, -18);
	test(*r->s4, 0, 1, "abcdefghijklmnopqrst", 20, -19);
	test(*r->s4, 0, 10, "", 0, 10);
	test(*r->s4, 0, 10, "abcde", 0, 10);
	test(*r->s4, 0, 10, "abcde", 1, 9);
	test(*r->s4, 0, 10, "abcde", 2, 8);
	test(*r->s4, 0, 10, "abcde", 4, 6);
	test(*r->s4, 0, 10, "abcde", 5, 5);
	test(*r->s4, 0, 10, "abcdefghij", 0, 10);
	test(*r->s4, 0, 10, "abcdefghij", 1, 9);
	test(*r->s4, 0, 10, "abcdefghij", 5, 5);
	test(*r->s4, 0, 10, "abcdefghij", 9, 1);
	test(*r->s4, 0, 10, "abcdefghij", 10, 0);
	test(*r->s4, 0, 10, "abcdefghijklmnopqrst", 0, 10);
	test(*r->s4, 0, 10, "abcdefghijklmnopqrst", 1, 9);
	test(*r->s4, 0, 10, "abcdefghijklmnopqrst", 10, 0);
	test(*r->s4, 0, 10, "abcdefghijklmnopqrst", 19, -9);
	test(*r->s4, 0, 10, "abcdefghijklmnopqrst", 20, -10);
	test(*r->s4, 0, 19, "", 0, 19);
	test(*r->s4, 0, 19, "abcde", 0, 19);
	test(*r->s4, 0, 19, "abcde", 1, 18);
	test(*r->s4, 0, 19, "abcde", 2, 17);
	test(*r->s4, 0, 19, "abcde", 4, 15);
	test(*r->s4, 0, 19, "abcde", 5, 14);
	test(*r->s4, 0, 19, "abcdefghij", 0, 19);
	test(*r->s4, 0, 19, "abcdefghij", 1, 18);
	test(*r->s4, 0, 19, "abcdefghij", 5, 14);
	test(*r->s4, 0, 19, "abcdefghij", 9, 10);
	test(*r->s4, 0, 19, "abcdefghij", 10, 9);
	test(*r->s4, 0, 19, "abcdefghijklmnopqrst", 0, 19);
	test(*r->s4, 0, 19, "abcdefghijklmnopqrst", 1, 18);
	test(*r->s4, 0, 19, "abcdefghijklmnopqrst", 10, 9);
	test(*r->s4, 0, 19, "abcdefghijklmnopqrst", 19, 0);
	test(*r->s4, 0, 19, "abcdefghijklmnopqrst", 20, -1);
	test(*r->s4, 0, 20, "", 0, 20);
	test(*r->s4, 0, 20, "abcde", 0, 20);
	test(*r->s4, 0, 20, "abcde", 1, 19);
	test(*r->s4, 0, 20, "abcde", 2, 18);
	test(*r->s4, 0, 20, "abcde", 4, 16);
	test(*r->s4, 0, 20, "abcde", 5, 15);
	test(*r->s4, 0, 20, "abcdefghij", 0, 20);
	test(*r->s4, 0, 20, "abcdefghij", 1, 19);
	test(*r->s4, 0, 20, "abcdefghij", 5, 15);
	test(*r->s4, 0, 20, "abcdefghij", 9, 11);
	test(*r->s4, 0, 20, "abcdefghij", 10, 10);
	test(*r->s4, 0, 20, "abcdefghijklmnopqrst", 0, 20);
	test(*r->s4, 0, 20, "abcdefghijklmnopqrst", 1, 19);
	test(*r->s4, 0, 20, "abcdefghijklmnopqrst", 10, 10);
	test(*r->s4, 0, 20, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s4, 0, 20, "abcdefghijklmnopqrst", 20, 0);
	test(*r->s4, 0, 21, "", 0, 20);
	test(*r->s4, 0, 21, "abcde", 0, 20);
	test(*r->s4, 0, 21, "abcde", 1, 19);
	test(*r->s4, 0, 21, "abcde", 2, 18);
	test(*r->s4, 0, 21, "abcde", 4, 16);
	test(*r->s4, 0, 21, "abcde", 5, 15);
	test(*r->s4, 0, 21, "abcdefghij", 0, 20);
	test(*r->s4, 0, 21, "abcdefghij", 1, 19);
	test(*r->s4, 0, 21, "abcdefghij", 5, 15);
	test(*r->s4, 0, 21, "abcdefghij", 9, 11);
	test(*r->s4, 0, 21, "abcdefghij", 10, 10);
	test(*r->s4, 0, 21, "abcdefghijklmnopqrst", 0, 20);
	test(*r->s4, 0, 21, "abcdefghijklmnopqrst", 1, 19);
	test(*r->s4, 0, 21, "abcdefghijklmnopqrst", 10, 10);
	test(*r->s4, 0, 21, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s4, 0, 21, "abcdefghijklmnopqrst", 20, 0);
	test(*r->s4, 1, 0, "", 0, 0);
	test(*r->s4, 1, 0, "abcde", 0, 0);
	test(*r->s4, 1, 0, "abcde", 1, -1);
	test(*r->s4, 1, 0, "abcde", 2, -2);
}

template <class S>
void
test9(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s4, 1, 0, "abcde", 4, -4);
	test(*r->s4, 1, 0, "abcde", 5, -5);
	test(*r->s4, 1, 0, "abcdefghij", 0, 0);
	test(*r->s4, 1, 0, "abcdefghij", 1, -1);
	test(*r->s4, 1, 0, "abcdefghij", 5, -5);
	test(*r->s4, 1, 0, "abcdefghij", 9, -9);
	test(*r->s4, 1, 0, "abcdefghij", 10, -10);
	test(*r->s4, 1, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s4, 1, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s4, 1, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s4, 1, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s4, 1, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s4, 1, 1, "", 0, 1);
	test(*r->s4, 1, 1, "abcde", 0, 1);
	test(*r->s4, 1, 1, "abcde", 1, 1);
	test(*r->s4, 1, 1, "abcde", 2, 1);
	test(*r->s4, 1, 1, "abcde", 4, 1);
	test(*r->s4, 1, 1, "abcde", 5, 1);
	test(*r->s4, 1, 1, "abcdefghij", 0, 1);
	test(*r->s4, 1, 1, "abcdefghij", 1, 1);
	test(*r->s4, 1, 1, "abcdefghij", 5, 1);
	test(*r->s4, 1, 1, "abcdefghij", 9, 1);
	test(*r->s4, 1, 1, "abcdefghij", 10, 1);
	test(*r->s4, 1, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s4, 1, 1, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s4, 1, 1, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s4, 1, 1, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s4, 1, 1, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s4, 1, 9, "", 0, 9);
	test(*r->s4, 1, 9, "abcde", 0, 9);
	test(*r->s4, 1, 9, "abcde", 1, 1);
	test(*r->s4, 1, 9, "abcde", 2, 1);
	test(*r->s4, 1, 9, "abcde", 4, 1);
	test(*r->s4, 1, 9, "abcde", 5, 1);
	test(*r->s4, 1, 9, "abcdefghij", 0, 9);
	test(*r->s4, 1, 9, "abcdefghij", 1, 1);
	test(*r->s4, 1, 9, "abcdefghij", 5, 1);
	test(*r->s4, 1, 9, "abcdefghij", 9, 1);
	test(*r->s4, 1, 9, "abcdefghij", 10, 1);
	test(*r->s4, 1, 9, "abcdefghijklmnopqrst", 0, 9);
	test(*r->s4, 1, 9, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s4, 1, 9, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s4, 1, 9, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s4, 1, 9, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s4, 1, 18, "", 0, 18);
	test(*r->s4, 1, 18, "abcde", 0, 18);
	test(*r->s4, 1, 18, "abcde", 1, 1);
	test(*r->s4, 1, 18, "abcde", 2, 1);
	test(*r->s4, 1, 18, "abcde", 4, 1);
	test(*r->s4, 1, 18, "abcde", 5, 1);
	test(*r->s4, 1, 18, "abcdefghij", 0, 18);
	test(*r->s4, 1, 18, "abcdefghij", 1, 1);
	test(*r->s4, 1, 18, "abcdefghij", 5, 1);
	test(*r->s4, 1, 18, "abcdefghij", 9, 1);
	test(*r->s4, 1, 18, "abcdefghij", 10, 1);
	test(*r->s4, 1, 18, "abcdefghijklmnopqrst", 0, 18);
	test(*r->s4, 1, 18, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s4, 1, 18, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s4, 1, 18, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s4, 1, 18, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s4, 1, 19, "", 0, 19);
	test(*r->s4, 1, 19, "abcde", 0, 19);
	test(*r->s4, 1, 19, "abcde", 1, 1);
	test(*r->s4, 1, 19, "abcde", 2, 1);
	test(*r->s4, 1, 19, "abcde", 4, 1);
	test(*r->s4, 1, 19, "abcde", 5, 1);
	test(*r->s4, 1, 19, "abcdefghij", 0, 19);
	test(*r->s4, 1, 19, "abcdefghij", 1, 1);
	test(*r->s4, 1, 19, "abcdefghij", 5, 1);
	test(*r->s4, 1, 19, "abcdefghij", 9, 1);
	test(*r->s4, 1, 19, "abcdefghij", 10, 1);
	test(*r->s4, 1, 19, "abcdefghijklmnopqrst", 0, 19);
	test(*r->s4, 1, 19, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s4, 1, 19, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s4, 1, 19, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s4, 1, 19, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s4, 1, 20, "", 0, 19);
	test(*r->s4, 1, 20, "abcde", 0, 19);
	test(*r->s4, 1, 20, "abcde", 1, 1);
	test(*r->s4, 1, 20, "abcde", 2, 1);
	test(*r->s4, 1, 20, "abcde", 4, 1);
	test(*r->s4, 1, 20, "abcde", 5, 1);
	test(*r->s4, 1, 20, "abcdefghij", 0, 19);
	test(*r->s4, 1, 20, "abcdefghij", 1, 1);
	test(*r->s4, 1, 20, "abcdefghij", 5, 1);
	test(*r->s4, 1, 20, "abcdefghij", 9, 1);
	test(*r->s4, 1, 20, "abcdefghij", 10, 1);
	test(*r->s4, 1, 20, "abcdefghijklmnopqrst", 0, 19);
	test(*r->s4, 1, 20, "abcdefghijklmnopqrst", 1, 1);
	test(*r->s4, 1, 20, "abcdefghijklmnopqrst", 10, 1);
	test(*r->s4, 1, 20, "abcdefghijklmnopqrst", 19, 1);
	test(*r->s4, 1, 20, "abcdefghijklmnopqrst", 20, 1);
	test(*r->s4, 10, 0, "", 0, 0);
	test(*r->s4, 10, 0, "abcde", 0, 0);
	test(*r->s4, 10, 0, "abcde", 1, -1);
	test(*r->s4, 10, 0, "abcde", 2, -2);
	test(*r->s4, 10, 0, "abcde", 4, -4);
	test(*r->s4, 10, 0, "abcde", 5, -5);
	test(*r->s4, 10, 0, "abcdefghij", 0, 0);
	test(*r->s4, 10, 0, "abcdefghij", 1, -1);
}

template <class S>
void
test10(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s4, 10, 0, "abcdefghij", 5, -5);
	test(*r->s4, 10, 0, "abcdefghij", 9, -9);
	test(*r->s4, 10, 0, "abcdefghij", 10, -10);
	test(*r->s4, 10, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s4, 10, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s4, 10, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s4, 10, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s4, 10, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s4, 10, 1, "", 0, 1);
	test(*r->s4, 10, 1, "abcde", 0, 1);
	test(*r->s4, 10, 1, "abcde", 1, 10);
	test(*r->s4, 10, 1, "abcde", 2, 10);
	test(*r->s4, 10, 1, "abcde", 4, 10);
	test(*r->s4, 10, 1, "abcde", 5, 10);
	test(*r->s4, 10, 1, "abcdefghij", 0, 1);
	test(*r->s4, 10, 1, "abcdefghij", 1, 10);
	test(*r->s4, 10, 1, "abcdefghij", 5, 10);
	test(*r->s4, 10, 1, "abcdefghij", 9, 10);
	test(*r->s4, 10, 1, "abcdefghij", 10, 10);
	test(*r->s4, 10, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s4, 10, 1, "abcdefghijklmnopqrst", 1, 10);
	test(*r->s4, 10, 1, "abcdefghijklmnopqrst", 10, 10);
	test(*r->s4, 10, 1, "abcdefghijklmnopqrst", 19, 10);
	test(*r->s4, 10, 1, "abcdefghijklmnopqrst", 20, 10);
	test(*r->s4, 10, 5, "", 0, 5);
	test(*r->s4, 10, 5, "abcde", 0, 5);
	test(*r->s4, 10, 5, "abcde", 1, 10);
	test(*r->s4, 10, 5, "abcde", 2, 10);
	test(*r->s4, 10, 5, "abcde", 4, 10);
	test(*r->s4, 10, 5, "abcde", 5, 10);
	test(*r->s4, 10, 5, "abcdefghij", 0, 5);
	test(*r->s4, 10, 5, "abcdefghij", 1, 10);
	test(*r->s4, 10, 5, "abcdefghij", 5, 10);
	test(*r->s4, 10, 5, "abcdefghij", 9, 10);
	test(*r->s4, 10, 5, "abcdefghij", 10, 10);
	test(*r->s4, 10, 5, "abcdefghijklmnopqrst", 0, 5);
	test(*r->s4, 10, 5, "abcdefghijklmnopqrst", 1, 10);
	test(*r->s4, 10, 5, "abcdefghijklmnopqrst", 10, 10);
	test(*r->s4, 10, 5, "abcdefghijklmnopqrst", 19, 10);
	test(*r->s4, 10, 5, "abcdefghijklmnopqrst", 20, 10);
	test(*r->s4, 10, 9, "", 0, 9);
	test(*r->s4, 10, 9, "abcde", 0, 9);
	test(*r->s4, 10, 9, "abcde", 1, 10);
	test(*r->s4, 10, 9, "abcde", 2, 10);
	test(*r->s4, 10, 9, "abcde", 4, 10);
	test(*r->s4, 10, 9, "abcde", 5, 10);
	test(*r->s4, 10, 9, "abcdefghij", 0, 9);
	test(*r->s4, 10, 9, "abcdefghij", 1, 10);
	test(*r->s4, 10, 9, "abcdefghij", 5, 10);
	test(*r->s4, 10, 9, "abcdefghij", 9, 10);
	test(*r->s4, 10, 9, "abcdefghij", 10, 10);
	test(*r->s4, 10, 9, "abcdefghijklmnopqrst", 0, 9);
	test(*r->s4, 10, 9, "abcdefghijklmnopqrst", 1, 10);
	test(*r->s4, 10, 9, "abcdefghijklmnopqrst", 10, 10);
	test(*r->s4, 10, 9, "abcdefghijklmnopqrst", 19, 10);
	test(*r->s4, 10, 9, "abcdefghijklmnopqrst", 20, 10);
	test(*r->s4, 10, 10, "", 0, 10);
	test(*r->s4, 10, 10, "abcde", 0, 10);
	test(*r->s4, 10, 10, "abcde", 1, 10);
	test(*r->s4, 10, 10, "abcde", 2, 10);
	test(*r->s4, 10, 10, "abcde", 4, 10);
	test(*r->s4, 10, 10, "abcde", 5, 10);
	test(*r->s4, 10, 10, "abcdefghij", 0, 10);
	test(*r->s4, 10, 10, "abcdefghij", 1, 10);
	test(*r->s4, 10, 10, "abcdefghij", 5, 10);
	test(*r->s4, 10, 10, "abcdefghij", 9, 10);
	test(*r->s4, 10, 10, "abcdefghij", 10, 10);
	test(*r->s4, 10, 10, "abcdefghijklmnopqrst", 0, 10);
	test(*r->s4, 10, 10, "abcdefghijklmnopqrst", 1, 10);
	test(*r->s4, 10, 10, "abcdefghijklmnopqrst", 10, 10);
	test(*r->s4, 10, 10, "abcdefghijklmnopqrst", 19, 10);
	test(*r->s4, 10, 10, "abcdefghijklmnopqrst", 20, 10);
	test(*r->s4, 10, 11, "", 0, 10);
	test(*r->s4, 10, 11, "abcde", 0, 10);
	test(*r->s4, 10, 11, "abcde", 1, 10);
	test(*r->s4, 10, 11, "abcde", 2, 10);
	test(*r->s4, 10, 11, "abcde", 4, 10);
	test(*r->s4, 10, 11, "abcde", 5, 10);
	test(*r->s4, 10, 11, "abcdefghij", 0, 10);
	test(*r->s4, 10, 11, "abcdefghij", 1, 10);
	test(*r->s4, 10, 11, "abcdefghij", 5, 10);
	test(*r->s4, 10, 11, "abcdefghij", 9, 10);
	test(*r->s4, 10, 11, "abcdefghij", 10, 10);
	test(*r->s4, 10, 11, "abcdefghijklmnopqrst", 0, 10);
	test(*r->s4, 10, 11, "abcdefghijklmnopqrst", 1, 10);
	test(*r->s4, 10, 11, "abcdefghijklmnopqrst", 10, 10);
	test(*r->s4, 10, 11, "abcdefghijklmnopqrst", 19, 10);
	test(*r->s4, 10, 11, "abcdefghijklmnopqrst", 20, 10);
	test(*r->s4, 19, 0, "", 0, 0);
	test(*r->s4, 19, 0, "abcde", 0, 0);
	test(*r->s4, 19, 0, "abcde", 1, -1);
	test(*r->s4, 19, 0, "abcde", 2, -2);
	test(*r->s4, 19, 0, "abcde", 4, -4);
	test(*r->s4, 19, 0, "abcde", 5, -5);
	test(*r->s4, 19, 0, "abcdefghij", 0, 0);
	test(*r->s4, 19, 0, "abcdefghij", 1, -1);
	test(*r->s4, 19, 0, "abcdefghij", 5, -5);
	test(*r->s4, 19, 0, "abcdefghij", 9, -9);
	test(*r->s4, 19, 0, "abcdefghij", 10, -10);
	test(*r->s4, 19, 0, "abcdefghijklmnopqrst", 0, 0);
}

template <class S>
void
test11(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	test(*r->s4, 19, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s4, 19, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s4, 19, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s4, 19, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s4, 19, 1, "", 0, 1);
	test(*r->s4, 19, 1, "abcde", 0, 1);
	test(*r->s4, 19, 1, "abcde", 1, 19);
	test(*r->s4, 19, 1, "abcde", 2, 19);
	test(*r->s4, 19, 1, "abcde", 4, 19);
	test(*r->s4, 19, 1, "abcde", 5, 19);
	test(*r->s4, 19, 1, "abcdefghij", 0, 1);
	test(*r->s4, 19, 1, "abcdefghij", 1, 19);
	test(*r->s4, 19, 1, "abcdefghij", 5, 19);
	test(*r->s4, 19, 1, "abcdefghij", 9, 19);
	test(*r->s4, 19, 1, "abcdefghij", 10, 19);
	test(*r->s4, 19, 1, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s4, 19, 1, "abcdefghijklmnopqrst", 1, 19);
	test(*r->s4, 19, 1, "abcdefghijklmnopqrst", 10, 19);
	test(*r->s4, 19, 1, "abcdefghijklmnopqrst", 19, 19);
	test(*r->s4, 19, 1, "abcdefghijklmnopqrst", 20, 19);
	test(*r->s4, 19, 2, "", 0, 1);
	test(*r->s4, 19, 2, "abcde", 0, 1);
	test(*r->s4, 19, 2, "abcde", 1, 19);
	test(*r->s4, 19, 2, "abcde", 2, 19);
	test(*r->s4, 19, 2, "abcde", 4, 19);
	test(*r->s4, 19, 2, "abcde", 5, 19);
	test(*r->s4, 19, 2, "abcdefghij", 0, 1);
	test(*r->s4, 19, 2, "abcdefghij", 1, 19);
	test(*r->s4, 19, 2, "abcdefghij", 5, 19);
	test(*r->s4, 19, 2, "abcdefghij", 9, 19);
	test(*r->s4, 19, 2, "abcdefghij", 10, 19);
	test(*r->s4, 19, 2, "abcdefghijklmnopqrst", 0, 1);
	test(*r->s4, 19, 2, "abcdefghijklmnopqrst", 1, 19);
	test(*r->s4, 19, 2, "abcdefghijklmnopqrst", 10, 19);
	test(*r->s4, 19, 2, "abcdefghijklmnopqrst", 19, 19);
	test(*r->s4, 19, 2, "abcdefghijklmnopqrst", 20, 19);
	test(*r->s4, 20, 0, "", 0, 0);
	test(*r->s4, 20, 0, "abcde", 0, 0);
	test(*r->s4, 20, 0, "abcde", 1, -1);
	test(*r->s4, 20, 0, "abcde", 2, -2);
	test(*r->s4, 20, 0, "abcde", 4, -4);
	test(*r->s4, 20, 0, "abcde", 5, -5);
	test(*r->s4, 20, 0, "abcdefghij", 0, 0);
	test(*r->s4, 20, 0, "abcdefghij", 1, -1);
	test(*r->s4, 20, 0, "abcdefghij", 5, -5);
	test(*r->s4, 20, 0, "abcdefghij", 9, -9);
	test(*r->s4, 20, 0, "abcdefghij", 10, -10);
	test(*r->s4, 20, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s4, 20, 0, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s4, 20, 0, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s4, 20, 0, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s4, 20, 0, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s4, 20, 1, "", 0, 0);
	test(*r->s4, 20, 1, "abcde", 0, 0);
	test(*r->s4, 20, 1, "abcde", 1, -1);
	test(*r->s4, 20, 1, "abcde", 2, -2);
	test(*r->s4, 20, 1, "abcde", 4, -4);
	test(*r->s4, 20, 1, "abcde", 5, -5);
	test(*r->s4, 20, 1, "abcdefghij", 0, 0);
	test(*r->s4, 20, 1, "abcdefghij", 1, -1);
	test(*r->s4, 20, 1, "abcdefghij", 5, -5);
	test(*r->s4, 20, 1, "abcdefghij", 9, -9);
	test(*r->s4, 20, 1, "abcdefghij", 10, -10);
	test(*r->s4, 20, 1, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s4, 20, 1, "abcdefghijklmnopqrst", 1, -1);
	test(*r->s4, 20, 1, "abcdefghijklmnopqrst", 10, -10);
	test(*r->s4, 20, 1, "abcdefghijklmnopqrst", 19, -19);
	test(*r->s4, 20, 1, "abcdefghijklmnopqrst", 20, -20);
	test(*r->s4, 21, 0, "", 0, 0);
	test(*r->s4, 21, 0, "abcde", 0, 0);
	test(*r->s4, 21, 0, "abcde", 1, 0);
	test(*r->s4, 21, 0, "abcde", 2, 0);
	test(*r->s4, 21, 0, "abcde", 4, 0);
	test(*r->s4, 21, 0, "abcde", 5, 0);
	test(*r->s4, 21, 0, "abcdefghij", 0, 0);
	test(*r->s4, 21, 0, "abcdefghij", 1, 0);
	test(*r->s4, 21, 0, "abcdefghij", 5, 0);
	test(*r->s4, 21, 0, "abcdefghij", 9, 0);
	test(*r->s4, 21, 0, "abcdefghij", 10, 0);
	test(*r->s4, 21, 0, "abcdefghijklmnopqrst", 0, 0);
	test(*r->s4, 21, 0, "abcdefghijklmnopqrst", 1, 0);
	test(*r->s4, 21, 0, "abcdefghijklmnopqrst", 10, 0);
	test(*r->s4, 21, 0, "abcdefghijklmnopqrst", 19, 0);
	test(*r->s4, 21, 0, "abcdefghijklmnopqrst", 20, 0);
}

void
run(pmem::obj::pool<root> &pop)
{
	auto r = pop.root();

	try {
		nvobj::transaction::run(pop, [&] {
			r->s1 = nvobj::make_persistent<pmem::obj::string>("");
			r->s2 = nvobj::make_persistent<pmem::obj::string>(
				"abcde");
			r->s3 = nvobj::make_persistent<pmem::obj::string>(
				"abcdefghij");
			r->s4 = nvobj::make_persistent<pmem::obj::string>(
				"abcdefghijklmnopqrst");
		});

		using S = pmem::obj::string;
		test0<S>(pop);
		test1<S>(pop);
		test2<S>(pop);
		test3<S>(pop);
		test4<S>(pop);
		test5<S>(pop);
		test6<S>(pop);
		test7<S>(pop);
		test8<S>(pop);
		test9<S>(pop);
		test10<S>(pop);
		test11<S>(pop);

		nvobj::transaction::run(pop, [&] {
			nvobj::delete_persistent<pmem::obj::string>(r->s1);
			nvobj::delete_persistent<pmem::obj::string>(r->s2);
			nvobj::delete_persistent<pmem::obj::string>(r->s3);
			nvobj::delete_persistent<pmem::obj::string>(r->s4);
		});
	} catch (std::exception &e) {
		UT_FATALexc(e);
	}
}

static void
test(int argc, char *argv[])
{
	if (argc != 2)
		UT_FATAL("usage: %s file-name", argv[0]);

	const char *path = argv[1];

	pmem::obj::pool<root> pop;

	try {
		pop = pmem::obj::pool<root>::create(path, "basic_string",
						    PMEMOBJ_MIN_POOL,
						    S_IWUSR | S_IRUSR);
	} catch (...) {
		UT_FATAL("!pmemobj_create: %s", path);
	}

	run(pop);

	pop.close();
}

int
main(int argc, char *argv[])
{
	return run_test([&] { test(argc, argv); });
}
