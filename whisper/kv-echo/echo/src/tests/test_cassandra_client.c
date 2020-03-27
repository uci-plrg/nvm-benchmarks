/* Peter Hornyack and Katelin Bailey
 * 11/10/11
 * University of Washington
 *
 * Test file for the Cassandra C client.
 */

#include "../cassandra_client.h"
#include "../kp_macros.h"

int main(int argc, char *argv[])
{
	int ret;
	char *retval = NULL;
	char *key1 = "key1";
	char *key2 = "key2";
	char *val1 = "value1";
	char *val2 = "val2";
	cassandra_node *node = NULL;
	long unsigned int tid = pthread_self();

	/* Connect to Cassandra instance (should already be running on
	 * localhost at CASSANDRA_PORT_DEFAULT:
	 */
	ret = cassandra_start(&node, CASSANDRA_PORT_DEFAULT);
	kp_testcase_int(tid, "cassandra_start", 0, ret);
	if (ret != 0) {
		kp_error("cassandra_start() returned error=%d\n", ret);
		return -1;
	}
	if (node == NULL) {
		kp_error("cassandra_start() did not set *node correctly\n");
		return -1;
	}
	kp_print("successfully connected to Cassandra node on port %u\n",
			CASSANDRA_PORT_DEFAULT);

	/* Test noops: */
	ret = cassandra_noop1(node);
	kp_testcase_int(tid, "cassandra_noop1", 0, ret);
	ret = cassandra_noop2(node);
	kp_testcase_int(tid, "cassandra_noop2", 0, ret);
	ret = cassandra_noop3(node);
	kp_testcase_int(tid, "cassandra_noop3", 0, ret);
	ret = cassandra_noop4(node);
	kp_testcase_int(tid, "cassandra_noop4", 0, ret);
	ret = cassandra_noop5(node);
	kp_testcase_int(tid, "cassandra_noop5", 0, ret);

	/* Get on empty store should return non-found: */
	ret = cassandra_get(node, key1, &retval);
	if (ret == 1) {
		kp_print("cassandra_get(%s) returned not-found (as expected)\n",
				key1);
	} else if (ret != 0) {
		kp_error("cassandra_get(%s) returned error=%d\n",
				key1, ret);
	} else {
		kp_error("cassandra_get(%s) returned 0, unexpectedly...\n",
				key1);
		if (retval) {
			kp_error("...with value=%s\n", retval);
			free(retval);
			retval = NULL;
		} else {
			kp_error("...but didn't set retval, wtf\n");
		}
	}

	/* Put some values: */
	ret = cassandra_put(node, key1, val1);
	if (ret != 0) {
		kp_error("cassandra_put(%s, %s) returned error=%d\n",
				key1, val1, ret);
	}
	ret = cassandra_put(node, key2, val2);
	if (ret != 0) {
		kp_error("cassandra_put(%s, %s) returned error=%d\n",
				key2, val2, ret);
	}

	/* Get some values: */
	ret = cassandra_get(node, key1, &retval);
	if (ret == 1) {
		kp_error("cassandra_get(%s) returned not-found!\n", key1);
	} else if (ret != 0) {
		kp_error("cassandra_get(%s) returned error=%d\n", key1, ret);
	} else {
		kp_print("cassandra_get(%s) returned value=%s\n", key1, retval);
		free(retval);
		retval = NULL;
	}
	ret = cassandra_get(node, key2, &retval);
	if (ret == 1) {
		kp_error("cassandra_get(%s) returned not-found!\n", key2);
	} else if (ret != 0) {
		kp_error("cassandra_get(%s) returned error=%d\n", key2, ret);
	} else {
		kp_print("cassandra_get(%s) returned value=%s\n", key2, retval);
		free(retval);
		retval = NULL;
	}

	/* Delete a value: */
#ifdef IMPLEMENTED
	ret = cassandra_delete(node, key1);
	if (ret == 1) {
		kp_error("cassandra_delete(%s) returned not-found!\n", key1);
	} else if (ret != 0) {
		kp_error("cassandra_delete(%s) returned error=%d\n", key1, ret);
	} else {
		kp_print("cassandra_delete(%s) returned success\n", key1);
	}
	ret = cassandra_get(node, key1, &retval);
	if (ret == 1) {
		kp_print("cassandra_get(%s) returned not-found (as expected)\n",
				key1);
	} else if (ret != 0) {
		kp_error("cassandra_get(%s) returned error=%d\n", key1, ret);
	} else {
		kp_error("cassandra_get(%s) returned 0, unexpectedly...\n", key1);
		if (retval) {
			kp_error("...with value=%s\n", retval);
			free(retval);
			retval = NULL;
		} else {
			kp_error("...but didn't set retval, wtf\n");
		}
	}
#endif
	ret = cassandra_get(node, key2, &retval);
	if (ret == 1) {
		kp_error("cassandra_get(%s) returned not-found!\n", key2);
	} else if (ret != 0) {
		kp_error("cassandra_get(%s) returned error=%d\n", key2, ret);
	} else {
		kp_print("cassandra_get(%s) returned value=%s\n", key2, retval);
		free(retval);
		retval = NULL;
	}

	/* Disconnect: */
	ret = cassandra_stop(node);
	if (ret != 0) {
		kp_error("cassandra_stop() returned error: %d\n", ret);
		return -1;
	}
	kp_print("successfully disconnected from Cassandra node\n");

	return 0;
}
