/* Peter Hornyack and Katelin Bailey
 * 11/4/11
 */

#include "cassandra.h"
#include <stdio.h>
#include <assert.h>
#include <sys/time.h>
#include <stdlib.h>
#include <string.h>
#include <transport/thrift_socket.h>
#include <transport/thrift_transport.h>
#include <transport/thrift_framed_transport.h>
#include <transport/thrift_server_transport.h>
#include <transport/thrift_server_socket.h>
#include <protocol/thrift_protocol.h>
#include <protocol/thrift_binary_protocol.h>
#include <glib.h>
#include <glib-object.h>
  //http://developer.gnome.org/glib/2.28/glib-compiling.html
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>

#define CASSANDRA_HOSTNAME "localhost"
#define CASSANDRA_PORT 9160  // port for Thrift to listen for clients on

void printstring(const char *label, const char *string)
{
	if (string != NULL) {
		printf("Print string: %s=[%s]\n", label, string);
	} else {
		printf("Print string: %s=NULL\n", label);
	}
}

void print_and_free_error(GError **err)
{
	if (*err) {
		printf("Print GError: %s\n", (*err)->message);
		g_error_free(*err);
		*err = NULL;
	} else {
		printf("No error :)\n");
	}
}

void printbool(gboolean retbool, const char *fn)
{
	if (!retbool) {
		printf("Print bool: %s() returned FALSE\n", fn);
	} else {
		printf("Print bool: %s() returned TRUE\n", fn);
	}
}

/* Takes a byte array that has already been created (g_byte_array_new() or
 * g_byte_array_sized_new()), resizes it to fit the string exactly
 * (WITHOUT the null-zero), and copies the string into it. chars are
 * casted to guint8s to fit into the byte array. The null-zero byte is
 * intentionally left off of the byte array, because otherwise Cassandra
 * CLI commands fail to get the inserted key.
 * Returns: 0 on success.
 */
int convert_string_to_bytearray(const char *string, GByteArray *bytes)
{
	int i, len;

	assert(sizeof(char) == sizeof(guint8));  /* check that casting makes sense */
	len = strlen(string);
	g_byte_array_set_size(bytes, len);
	assert(bytes->len == len);
	for (i = 0; i < len; i++) {
		(bytes->data)[i] = (guint8)(string[i]);
	}
	/* This is the difference between when the null byte is stored in the
	 * byte array (testkey01) and when it is not (testkey02):
	 *   [default@Keyspace1] get Users['testkey01'];
	 *     Returned 0 results.
	 *   [default@Keyspace1] get Users['testkey02'];
	 *     => (column=testcolname01, value=testcolvalue01,
	 *         timestamp=1292975298)
	 */

	return 0;
}

/* Converts a bytearray to a string. This function allocates space for the
 * new string, which is len characters long plus one more byte for the
 * null-zero. The caller should free the pointer that is returned.
 * Returns: 0 on success, -1 on error.
 */
int convert_bytearray_to_string(GByteArray *bytes, char **string)
{
	int i;
	unsigned int len;

	assert(sizeof(char) == sizeof(guint8));  /* check that casting makes sense */
	len = bytes->len;
	*string = malloc(len+1);
	if (!(*string)) {
		return 1;
	}

	for (i = 0; i < len; i++) {
		(*string)[i] = (char)((bytes->data)[i]);
	}
	(*string)[len] = '\0';
	printf("converted byte array to string: %s\n", *string);

	return 0;
}

int main(int argc, char *argv[])
{
	int i, ret;
	ThriftSocket *tsocket = NULL;
	ThriftFramedTransport *framedtransport = NULL;
	ThriftTransport *transport = NULL;
	ThriftBinaryProtocol *binaryprotocol = NULL;
	ThriftProtocol *protocol = NULL;
	CassandraClient *client = NULL;
	CassandraIf *iface = NULL;
	gboolean retbool;
	gchar *retstring;
	GError *err = NULL;
	InvalidRequestException *invalidreq_except;
	UnavailableException *unavail_except;
	TimedOutException *timedout_except;
	SchemaDisagreementException *schemadisagree_except;
	NotFoundException *notfound_except;
	GPtrArray *retarray;

	/* Initialize GObject and GType shit */
	g_type_init ();

	/* Set up the exception pointers. DON'T allocate an exception now
	 * using g_object_new(TYPE_INVALID_REQUEST_EXCEPTION); if you do this
	 * and then pass it to the cassandra_if_*() functions, they all will
	 * fail!
	 * I have determined empirically that the proper way to handle these
	 * exceptions and this GError is to first make sure that ALL of them
	 * are set to NULL before calling any of the cassandra_if_*()
	 * functions, otherwise these functions will fail and return FALSE,
	 * even if they should succeed. If a function does fail, it should
	 * return FALSE, set the GError struct and set its pointer to
	 * non-NULL, and possibly set one of the exceptions and set its
	 * pointer to non-NULL. After checking and printing the why/message
	 * fields, any exceptions that were set should be freed using
	 * g_object_unref() and their pointers must be set to NULL, and then
	 * the GError should be freed with g_error_free() and its pointer set
	 * to NULL as well.
	 */
	invalidreq_except = NULL;
	unavail_except = NULL;
	timedout_except = NULL;
	schemadisagree_except = NULL;
	notfound_except = NULL;
	err = NULL;

	printf("Attempting to connect to server %s:%d\n",
			CASSANDRA_HOSTNAME, CASSANDRA_PORT);
	tsocket = (ThriftSocket *) g_object_new (THRIFT_TYPE_SOCKET,
			"hostname", CASSANDRA_HOSTNAME,
			"port", CASSANDRA_PORT, NULL);
	assert (tsocket != NULL);

	/* See test/testframedtransport.c in Thrift source for some sample
	 * code for TFramedTransport client. Allocate a new
	 * ThriftFramedTransport object: its properties (e.g. w_buf_size) are
	 * defined in include/thrift/transport/thrift_framed_transport.h. */
	framedtransport = g_object_new (THRIFT_TYPE_FRAMED_TRANSPORT,
			"transport", THRIFT_TRANSPORT (tsocket),
			"w_buf_size", 4, NULL);
	  /* Note: THRIFT_TRANSPORT(tsocket) is a CAST from ThriftSocket to
	   *   ThriftTransport!!! */

#if 0
	/* DEBUG: we open the transport later!! */
	retbool = thrift_framed_transport_open(framedtransport, NULL);
	printbool(retbool, "thrift_framed_transport_open");
	retbool = thrift_framed_transport_is_open(framedtransport);
	printbool(retbool, "thrift_framed_transport_is_open");
	abort();  /* Looks like these work when Cassandra is running. */
#endif

	/* Cast from ThriftFramedTransport to ThriftTransport.
	 * THRIFT_TRANSPORT is defined in
	 * include/thrift/transport/thrift_transport.h. */
	transport = THRIFT_TRANSPORT(framedtransport);

	/* See test/testbinaryprotocol.c in Thrift source for some sample code
	 * for ThriftBinaryProtocol. At lines 409 and 501, a
	 * THRIFT_TYPE_BINARY_PROTOCOL is used/created with a ThriftTransport,
	 * rather than a ThriftSocket, which is what we're trying to do here
	 * (we're also trying to imitate
	 * http://wiki.apache.org/cassandra/ThriftExamples#C.2B-.2B-): */
	binaryprotocol = (ThriftBinaryProtocol *) g_object_new (THRIFT_TYPE_BINARY_PROTOCOL,
			"transport", transport, NULL);
	  /* Note: the "transport" property of ThriftBinaryProtocol is
	   * specified for its (abstract) parent class, ThriftProtocol, in
	   * include/thrift/protocol/thrift_protocol.h (L86). It is expected
	   * to be of type ThriftTransport. */

	/* Cast from ThriftBinaryProtocol to ThriftProtocol: */
	protocol = THRIFT_PROTOCOL(binaryprotocol);

	/* Create Cassandra client and interface objects: */
	client = (CassandraClient *) g_object_new (TYPE_CASSANDRA_CLIENT, "input_protocol", protocol, "output_protocol", protocol, NULL);
	  /* Definition of struct _CassandraClient in cassandra.h shows that
	   * input_protocol and output_protocol should be ThriftProtocols! */
	iface = CASSANDRA_IF (client);
	assert(iface != NULL);

	/* Open and send: */
	retbool = thrift_transport_open (transport, NULL);
	printbool(retbool, "thrift_transport_open");
	  /* It works! This only returns TRUE when cassandra is running. */
	if (!retbool) {
		printf("Transport did not open (Cassandra instance is probably "
				"not running), so exiting\n");
		exit(1);
	}

	/* At this point, use cassandra_if_*() commands (cassandra.h), and
	 * then when finished, call thrift_transport_close(). The
	 * cassandra_if_*() commands lead to the cassandra_client_*()
	 * commands, which wrap around each cassandra_client_send_*() and
	 * cassandra_client_recv_*(). All of these commands are described
	 * here: http://wiki.apache.org/cassandra/API .
	 */
	retstring = malloc(512);  //TODO: fix this, obviously
	retbool = cassandra_if_describe_version(iface, &retstring, &err);
	printbool(retbool, "cassandra_if_describe_version");
	printstring("version", retstring);
	print_and_free_error(&err);
	free(retstring);

	retarray = g_ptr_array_new();
	KsDef *ksdef;  //Keyspace definition
	retbool = cassandra_if_describe_keyspaces(iface, &retarray, &invalidreq_except, &err);
	printbool(retbool, "cassandra_if_describe_keyspaces");
	if (invalidreq_except) {
		printstring("invalidreq_except->why", invalidreq_except->why);
		g_object_unref(invalidreq_except);
		invalidreq_except = NULL;
	}
	print_and_free_error(&err);
	for (i = 0; i < retarray->len; i++) {
		ksdef = (KsDef *)g_ptr_array_index(retarray, i);
		if (ksdef) {
			printf("Keyspace name=[%s]\n", ksdef->name);
		} else {
			printf("Keyspace pointer is null!?\n");
		}
	}
	g_ptr_array_free(retarray, TRUE);

	/* Set keyspace via interface: */
	char *keyspace = "Keyspace1";
	retbool = cassandra_if_set_keyspace(iface, keyspace, &invalidreq_except, &err);
	  /* For debugging/testing, "system" looks like it should always be a
	   * valid keyspace. */
	printbool(retbool, "cassandra_if_set_keyspace");
	if (invalidreq_except) {
		printstring("invalidreq_except->why", invalidreq_except->why);
		g_object_unref(invalidreq_except);
		invalidreq_except = NULL;
	}
	print_and_free_error(&err);

	/* To match a simple key-value store, seems like there are two
	 * choices (see
	 * https://sampa.cs.washington.edu/sampa-private/Comparison_to_other_KV_stores):
	 *   Single keyspace
	 *   Single column family
	 *   Either:
	 *     - Row keys that match our keys, column names that match our
	 *       values, column values that are empty
	 *     - Row keys that match our keys, a single column name, and
	 *       column values that match our values.
	 *   Additionally, each column has a timestamp as well.
	 */

	/* Unfortunately, if the column family does not already exist in the
	 * keyspace, then we have to create it first; cassandra_if_insert()
	 * won't automatically add it. CfDef has a bazillion fields
	 * (http://wiki.apache.org/cassandra/API#CfDef); the code below matches
	 * the default values that are used for a simple ColumnFamily created
	 * from Cassandra CLI (run "describe Keyspace1;" to see these values).
	 */
	CfDef *cf_def = (CfDef *)g_object_new(TYPE_CF_DEF, NULL);
	cf_def->keyspace = keyspace;
	char *col_family_name = "TestColFamily07";
	  /* If it already exists, will get error:
	   *   Print string: invalidreq_except->why=[TestColFamily03 already
	   *     exists in keyspace Keyspace1]
	   */
	cf_def->name = col_family_name;
	char *column_type = "Standard";
	  /* Cassandra source java/org/apache/cassandra/db/ColumnFamilyType.java:
	   * choices are "Standard" or "Super." (this is not UTF8Type vs.
	   * BytesType vs. whatever)
	   */
	cf_def->column_type = column_type;
	cf_def->__isset_column_type = TRUE;
	char *comparator_type = "UTF8Type";
	cf_def->comparator_type = comparator_type;
	cf_def->__isset_comparator_type = TRUE;
	cf_def->__isset_subcomparator_type = FALSE;
	cf_def->__isset_comment = FALSE;
	cf_def->__isset_row_cache_size = FALSE;
	cf_def->__isset_key_cache_size = FALSE;
	cf_def->__isset_read_repair_chance = FALSE;
#if 0
	ColumnDef *cdef = (ColumnDef *)g_object_new(TYPE_COLUMN_DEF, NULL);
	const char *cname = col_family_name;    //?????????????????????????????
	GByteArray *cname_bytearray = g_byte_array_new();
	convert_string_to_bytearray(cname, cname_bytearray);
	cdef->name = cname_bytearray;  //must be defined, or will cause segmentation fault
	char *validation_class = "UTF8Type";  //see Cassandra source: resources/org/apache/cassandra/cli/CliHelp.yaml
	cdef->validation_class = validation_class;
	cdef->__isset_index_type = FALSE;
	cdef->__isset_index_name = FALSE;
	cdef->__isset_index_options = FALSE;
	GPtrArray *column_metadata = g_ptr_array_new();
	g_ptr_array_add(column_metadata, (gpointer)cdef);
	cf_def->column_metadata = column_metadata;
	cf_def->__isset_column_metadata = TRUE;
#endif
	cf_def->__isset_column_metadata = FALSE;
	cf_def->__isset_gc_grace_seconds = FALSE;
	char *default_validation_class = "UTF8Type";
	cf_def->default_validation_class = default_validation_class;
	cf_def->__isset_default_validation_class = TRUE;
	cf_def->__isset_id = FALSE;
	cf_def->__isset_min_compaction_threshold = FALSE;
	cf_def->__isset_max_compaction_threshold = FALSE;
	cf_def->__isset_row_cache_save_period_in_seconds = FALSE;
	cf_def->__isset_key_cache_save_period_in_seconds = FALSE;
	cf_def->__isset_replicate_on_write = FALSE;
	cf_def->__isset_merge_shards_chance = FALSE;
	char *key_validation_class = "UTF8Type";
	cf_def->key_validation_class = key_validation_class;
	cf_def->__isset_key_validation_class = TRUE;
	cf_def->__isset_row_cache_provider = FALSE;
	cf_def->__isset_key_alias = FALSE;
	cf_def->__isset_compaction_strategy = FALSE;
	cf_def->__isset_compaction_strategy_options = FALSE;
	cf_def->__isset_row_cache_keys_to_save = FALSE;
	cf_def->__isset_compression_options = FALSE;

	/* Drop the column family first, then add it. If the column family
	 * doesn't exist yet, dropping it will return an
	 * InvalidRequestException: "CF is not defined in that keyspace."
	 */
	retstring = malloc(512);  //TODO: fix this...
	retbool = cassandra_if_system_drop_column_family(iface, &retstring, col_family_name, &invalidreq_except, &schemadisagree_except, &err);
	printbool(retbool, "cassandra_if_system_drop_column_family");
	printstring("new schema version ID", retstring);
	if (invalidreq_except) {
		printstring("invalidreq_except->why", invalidreq_except->why);
		g_object_unref(invalidreq_except);
		invalidreq_except = NULL;
	}
	if (schemadisagree_except) {
		printf("cassandra_if_system_drop_column_family() set a SchemaDisagreementException!\n");
		g_object_unref(schemadisagree_except);
		schemadisagree_except = NULL;
	}
	print_and_free_error(&err);
	retbool = cassandra_if_system_add_column_family(iface, &retstring, cf_def, &invalidreq_except, &schemadisagree_except, &err);
	printbool(retbool, "cassandra_if_system_add_column_family");
	printstring("new schema version ID", retstring);
	if (invalidreq_except) {
		printstring("invalidreq_except->why", invalidreq_except->why);
		g_object_unref(invalidreq_except);
		invalidreq_except = NULL;
	}
	if (schemadisagree_except) {
		printf("cassandra_if_system_add_column_family() set a SchemaDisagreementException!\n");
		g_object_unref(schemadisagree_except);
		schemadisagree_except = NULL;
	}
	print_and_free_error(&err);
	free(retstring);
	//g_object_unref(cdef);
	g_object_unref(cf_def);
	//g_ptr_array_free(column_metadata, TRUE);
	/* IMPORTANT: note that after cassandra_if_system_add_column_family()
	 * successfully returns, "list" or other commands on the new CF may
	 * return "<column name> not found in current keyspace." This can be
	 * solved by restarting the Cassandra instance and/or CLI.
	 */
	//TODO: sleep for a little while here, to allow compaction after
	//  dropping CF to happen, and to allow added column family to be
	//  initialized...?

	/* Set up the key: an array of bytes. To test if the insert worked
	 * using the Cassandra CLI: "get Users['testkey01'];" or "list Users;"
	 * (note that "list Users;" maps to a RangeSliceCommand: see DEBUG
	 * output from Cassandra when running this command). */
	const char *testkey = "testkey02";
	GByteArray *key_bytearray = g_byte_array_new();
	convert_string_to_bytearray(testkey, key_bytearray);

	/* Set up the column: there are no protected/private
	 * "properties" to set at g_object_new() time, but we set the public
	 * members after the object has been created.*/
	ColumnParent *col_parent =
		(ColumnParent *)g_object_new(TYPE_COLUMN_PARENT, NULL);
	col_parent->column_family = col_family_name;  //gchar *
	col_parent->__isset_super_column = FALSE;     //gboolean
	    /* (see cassandra_types.h:434) */
	
	/* Set up the column: a single column name, and column values that
	 * match our values. Don't set the ttl. */
	const char *test_col_name = "testcolname01";
	GByteArray *col_name = g_byte_array_new();
	convert_string_to_bytearray(test_col_name, col_name);
	const char *test_col_value = "testcolvalue01";
	GByteArray *col_value = g_byte_array_new();
	convert_string_to_bytearray(test_col_value, col_value);
	struct timeval tv;
	ret = gettimeofday(&tv, NULL);
	if (ret != 0) {
		printf("error: gettimeofday() returned error %d\n", ret);
		abort();
	}
	printf("timeval: tv_sec=%ld, tv_usec=%ld\n", tv.tv_sec, tv.tv_usec);
#if 0
	gint64 timestamp = tv.tv_sec*1000000 + tv.tv_usec;
	printf("calculated timestamp in microseconds: %lld\n", timestamp);
#endif
#if 0
	gint64 timestamp = tv.tv_sec*1000 + tv.tv_usec/1000;
	printf("calculated timestamp in milliseconds: %lld\n", timestamp);
#endif
	gint64 timestamp = tv.tv_sec;
	printf("calculated timestamp in seconds: %lld\n", timestamp);
	Column *column = (Column *)g_object_new(TYPE_COLUMN, NULL);
	column->name = col_name;  //GByteArray *
	column->value = col_value;  //GByteArray *
	column->__isset_value = TRUE;  //gboolean
	column->timestamp = timestamp;  //gint64, stupid; should be unsigned!!
	column->__isset_timestamp = TRUE;  //gboolean
	column->__isset_ttl = FALSE;  //gboolean
	    /* (see cassandra_types.h:75) */

	/* ConsistencyLevel is an enum, not a struct (cassandra_types.h:28);
	 * see http://wiki.apache.org/cassandra/API#ConsistencyLevel 
	 * CONSISTENCY_LEVEL_QUORUM means "Ensure that the write has been
	 * written to N / 2 + 1 replicas before responding to the client.";
	 * CONSISTENCY_LEVEL_ONE means "Ensure that the write has been written
	 * to at least 1 replica's commit log and memory table before
	 * responding to the client."; presumably this has the least latency. */
	//ConsistencyLevel consistency_level_write = CONSISTENCY_LEVEL_QUORUM;
	ConsistencyLevel consistency_level_write = CONSISTENCY_LEVEL_ONE;

	/* The actual put() command: */
	retbool = cassandra_if_insert(iface, key_bytearray, col_parent, column, consistency_level_write, &invalidreq_except, &unavail_except, &timedout_except, &err);
	printbool(retbool, "cassandra_if_insert");
	if (invalidreq_except) {
		printstring("invalidreq_except->why", invalidreq_except->why);
		g_object_unref(invalidreq_except);
		invalidreq_except = NULL;
	}
	if (unavail_except) {
		printf("cassandra_if_insert() returned an UnavailableException!\n");
		g_object_unref(unavail_except);
		unavail_except = NULL;
	}
	if (timedout_except) {
		printf("cassandra_if_insert() returned a TimedOutException!\n");
		g_object_unref(timedout_except);
		timedout_except = NULL;
	}
	print_and_free_error(&err);
	g_byte_array_free(col_value, TRUE);
	g_object_unref(col_parent);
	g_object_unref(column);

	/* Get the value that we just put. Re-use the GByteArray for the key
	 * (key_bytearray). For a Cassandra instance running on just one node,
	 * I don't think the ConsistencyLevel means anything.
	 */
	ColumnPath *column_path = (ColumnPath *)g_object_new(TYPE_COLUMN_PATH, NULL);
	  /* ColumnPath is a combination of ColumnParent and Column */
	column_path->column_family = col_family_name;  //gchar *
	column_path->__isset_super_column = FALSE;
	column_path->column = col_name;  //GByteArray *   necessary??
	column_path->__isset_column = TRUE;
	ConsistencyLevel consistency_level_read = CONSISTENCY_LEVEL_ONE;
	  //todo: try CONSISTENCY_LEVEL_ALL and see if there's any difference
	  //  (there shouldn't be, since we're running on just one node).
	ColumnOrSuperColumn *col_or_super_col = NULL;
	retbool = cassandra_if_get(iface, &col_or_super_col, key_bytearray,
			column_path, consistency_level_read, &invalidreq_except,
			&notfound_except, &unavail_except, &timedout_except,
			&err);
	printbool(retbool, "cassandra_if_get");
	/* Print and free the result
	 * (http://wiki.apache.org/cassandra/API#ColumnOrSuperColumn): */
	if (col_or_super_col) {
		if (col_or_super_col->__isset_column) {
			char *get_name;
			char *get_value;
			int ret;
			ret = convert_bytearray_to_string(col_or_super_col->column->name, &get_name);
			if (ret != 0) {
				printf("convert_bytearray_to_string() returned error=%d\n", ret);
				exit(EXIT_FAILURE);
			}
			ret = convert_bytearray_to_string(col_or_super_col->column->value, &get_value);
			if (ret != 0) {
				printf("convert_bytearray_to_string() returned error=%d\n", ret);
				exit(EXIT_FAILURE);
			}
			printf("Got column name=%s, value=%s\n", get_name, get_value);
			free(get_name);
			free(get_value);
		}
		if (col_or_super_col->__isset_super_column) {
			printf("WARNING: unexpectedly got a SuperColumn!! "
					"Doing nothing with it.\n");
		}
		if (!col_or_super_col->__isset_column && !col_or_super_col->__isset_super_column) {
			printf("WARNING: got neither a column nor a super column, this "
					"should never happen\n");
		}
		g_object_unref(col_or_super_col);
	}
	if (invalidreq_except) {
		printstring("invalidreq_except->why", invalidreq_except->why);
		g_object_unref(invalidreq_except);
		invalidreq_except = NULL;
	}
	if (notfound_except) {
		printf("cassandra_if_get() returned NotFoundException!\n");
		g_object_unref(notfound_except);
		notfound_except = NULL;
	}
	if (unavail_except) {
		printf("cassandra_if_get() returned UnavailableException!\n");
		g_object_unref(unavail_except);
		unavail_except = NULL;
	}
	if (timedout_except) {
		printf("cassandra_if_get() returned TimedOutException!\n");
		g_object_unref(timedout_except);
		timedout_except = NULL;
	}
	g_object_unref(column_path);
	print_and_free_error(&err);

	/* Drop the column family again, now that we're done: */
	retstring = malloc(512);  //TODO: fix this...
	retbool = cassandra_if_system_drop_column_family(iface, &retstring, col_family_name, &invalidreq_except, &schemadisagree_except, &err);
	printbool(retbool, "cassandra_if_system_drop_column_family");
	printstring("new schema version ID", retstring);
	if (invalidreq_except) {
		printstring("invalidreq_except->why", invalidreq_except->why);
		g_object_unref(invalidreq_except);
		invalidreq_except = NULL;
	}
	if (schemadisagree_except) {
		printf("cassandra_if_system_drop_column_family() set a SchemaDisagreementException!\n");
		g_object_unref(schemadisagree_except);
		schemadisagree_except = NULL;
	}
	print_and_free_error(&err);
	free(retstring);

	/* Close transport: */
	retbool = thrift_transport_close (transport, NULL);
	printbool(retbool, "thrift_transport_close");

	//TODO: free exception, error objects, byte arrays, strings...
	g_byte_array_free(col_name, TRUE);
	g_byte_array_free(key_bytearray, TRUE);
	g_object_unref(client);
	g_object_unref(binaryprotocol);
	g_object_unref(framedtransport);
	g_object_unref(tsocket);

	return 0;
}

#if 0
	//	int status;
	//	pid_t pid;
	ThriftSocket *tsocket = NULL;
	ThriftTransport *transport = NULL;
	int port = 51199;
	guchar buf[10] = TEST_DATA; /* a buffer */
	GError *err = NULL;

	g_type_init();


	tsocket = g_object_new (THRIFT_TYPE_SOCKET, "hostname", "localhost",
			"port", port, NULL);
	transport = THRIFT_TRANSPORT (tsocket);
	assert (thrift_socket_open (transport, NULL) == TRUE);
	/* This will be FALSE if socket's hostname lookup failed */
	assert (thrift_socket_is_open (transport));

	thrift_socket_write (transport, buf, 10, NULL);
	thrift_socket_write_end (transport, NULL);
	thrift_socket_flush (transport, NULL);
	thrift_socket_close (transport, NULL);
	assert (thrift_socket_open (transport, &err) == FALSE);
	g_object_unref (tsocket);
	g_error_free (err);
#endif

#if 0
	//transport = THRIFT_TRANSPORT (tsocket);
	retbool = thrift_socket_open(transport, &err);
	if (retbool == FALSE) {
		/* This will be FALSE if socket's hostname lookup failed */
		/* This check always gives this error for some reason, whether or
		 * not it returns TRUE or FALSE:
		 * ** (process:18116): CRITICAL **: thrift_socket_open: assertion `tsocket->sd == 0' failed
		 */
		printf("thrift_socket_open() returned FALSE\n");
		g_error_free(err);
	} else {
		/* It works! thrift_socket_open() returns TRUE when cassandra is
		 * running and listening for Thrift connections on port 9160, but
		 * returns FALSE at other times. */
		printf("thrift_socket_open() returned TRUE!\n");
	}
	if (thrift_socket_is_open (transport) == FALSE) {
		/* This check always returns TRUE, whether or not cassandra is
		 * running... */
		printf("thrift_socket_is_open() returned FALSE\n");
		abort();
	} else {
		printf("thrift_socket_is_open() returned TRUE!\n");
	}

	//protocol = (ThriftBinaryProtocol *) g_object_new (THRIFT_TYPE_BINARY_PROTOCOL,
	//		"transport",
	//		tsocket, NULL);
#endif


