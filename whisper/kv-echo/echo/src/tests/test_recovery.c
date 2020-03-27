/* Peter Hornyack and Katelin Bailey
 * 3/10/12
 * University of Washington
 */

#include <stdlib.h>
#include "../recovery.h"
#include "../vector.h"

typedef struct data_structure_ {
	char *name;
	int num1;
	ds_state state;
} data_structure;

void dump_data_structure(data_structure *ds)
{
	int i, ds_int;
	int *ds_int_ptr;
	r_print("data structure (%p): name=%s, num1=%d, state=%d\n",
			ds, ds->name, ds->num1, ds->state);
	ds_int = (int)ds;
	for (i = 0; i < sizeof(data_structure); i += sizeof(int)) {
		ds_int_ptr = (int *)(ds_int + i);
		r_print("%p: %X\n", ds_int_ptr, *ds_int_ptr);
		  //print 4 bytes at a time
	}
}

void test_vector()
{
	bool use_nvm = false;
	vector *v;
	v = NULL;
	r_print("value of v initially is: %p\n", v);

	vector_create(&v, 0, use_nvm);
	r_print("value of v after creation is: %p\n", v);

	vector_destroy(&v);
	r_print("value of v after destroy is: %p\n", v);
	if (v != NULL) {
		r_error("v is not NULL as expected!\n");
		exit(-1);
	}
}

int main(int argc, char *argv[])
{
	bool use_nvm = false;
	data_structure *ds;

	kp_calloc((void **)(&ds), sizeof(data_structure), use_nvm);
	if (!ds) {
		r_error("kp_calloc failed\n");
		return -1;
	}
	dump_data_structure(ds);
	if (ds->name != NULL || ds->num1 != 0 || ds->state != STATE_ALLOCATED) {
		r_error("unexpected state (0)\n");
		return -1;
	}

	ds->num1 = ds->num1 + 1;
	set_ds_state_active(ds, offsetof(data_structure, state));
	dump_data_structure(ds);
	if (ds->name != NULL || ds->num1 != 1 || ds->state != STATE_ACTIVE) {
		r_error("unexpected state (1)\n");
		return -1;
	}

	ds->num1 = ds->num1 + 1;
	set_ds_state_dead(ds, offsetof(data_structure, state));
	dump_data_structure(ds);
	if (ds->name != NULL || ds->num1 != 2 || ds->state != STATE_DEAD) {
		r_error("unexpected state (2)\n");
		return -1;
	}

	kp_free((void **)(&ds), use_nvm);

	test_vector();

	return 0;
}
