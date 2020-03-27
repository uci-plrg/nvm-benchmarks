/* c-basic-offset: 2; tab-width: 2; indent-tabs-mode: t
 * vi: set noexpandtab:
 * :noTabs=false:
 * 
 * Peter Hornyack and Katelin Bailey
 * 11/10/11
 * University of Washington
 *
 * Implementation of a basic C client for Cassandra.
 */

#include "cassandra_client.h"
#include "cassandra.h"
#include "kp_macros.h"
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
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <glib.h>
#include <glib-object.h>
  //http://developer.gnome.org/glib/2.28/glib-compiling.html

#define RETSTRING_LENGTH 512
  /* Not sure this is safe, but good enough. */
#define CASSANDRA_HOSTNAME "localhost"
  /* Haven't tried with anything else yet. */
#define CASSANDRA_KEYSPACE "Keyspace1"
  /* Haven't tried anything else. */
#define CASSANDRA_COL_FAM_NAME "C_Client_Column_Family"
  /* Make sure this doesn't match anything in your Cassandra store,
   * because cassandra_start() will drop this column family! */
#define CASSANDRA_KEY_NAME "c_client_key"
#define CASSANDRA_COL_NAME "c_client_col"
#define CASSANDRA_COL_VALUE "c_client_col_val"
#define CASSANDRA_COMPARATOR_TYPE "UTF8Type"
#define CASSANDRA_DEFAULT_VALIDATION_CLASS "UTF8Type"
#define CASSANDRA_KEY_VALIDATION_CLASS "UTF8Type"

//#define CASSANDRA_CONSISTENCY_WRITE CONSISTENCY_LEVEL_QUORUM
#define CASSANDRA_CONSISTENCY_WRITE CONSISTENCY_LEVEL_ONE
  /* CONSISTENCY_LEVEL_QUORUM means "Ensure that the write has been
   * written to N / 2 + 1 replicas before responding to the client.";
   * CONSISTENCY_LEVEL_ONE means "Ensure that the write has been written
   * to at least 1 replica's commit log and memory table before
   * responding to the client."; presumably this has the least latency.
   */

//#define CASSANDRA_CONSISTENCY_READ CONSISTENCY_LEVEL_ALL
#define CASSANDRA_CONSISTENCY_READ CONSISTENCY_LEVEL_ONE
  //todo: try CONSISTENCY_LEVEL_ALL and see if there's any difference
  //  (there shouldn't be, since we're running on just one node).

//#define CASSANDRA_DATA_MODEL DM_ROWKEYS_USECOLVALS
//#define CASSANDRA_DATA_MODEL DM_ROWKEYS_USECOLNAMES
  /* Does not work currently! On get, gives an error:
   *   cassandra_if_get(key1) returned InvalidRequestException: column
   *   parameter is not optional for standard CF
   * I think this means that for this data model, we need to do
   * get_slice(), rather than just get(), to avoid having to name the
   * column that we want to get (because the column name will store
   * the VALUE that we want).
   *   gboolean cassandra_if_get_slice (CassandraIf *iface,
   *       GPtrArray ** _return, const GByteArray * key,
   *       const ColumnParent * column_parent, const SlicePredicate *
   *       predicate, const ConsistencyLevel consistency_level,
   *       InvalidRequestException ** ire, UnavailableException ** ue,
   *       TimedOutException ** te, GError **error);
   * To get all columns, see L65-67 of the C++ code example on this page:
   * http://wiki.apache.org/cassandra/ThriftExamples.
   * Also: http://stackoverflow.com/questions/2708425/how-to-get-properties-from-cassandra-with-get-slice-in-erlang
   * ALSO, put() gives this error: "cassandra_if_insert(key1, value1)
   *   returned InvalidRequestException: Column value is required".
   */
#define CASSANDRA_DATA_MODEL DM_COLNAMEKEYS

/* Global variables: */
unsigned int cassandra_node_id_count = 1234;

/* External structures: */
struct cassandra_node_struct {
	unsigned int id;
	char *hostname;
	unsigned int port;
	ThriftSocket *tsocket;
	ThriftFramedTransport *tframedtransport;
	ThriftTransport *ttransport;
	ThriftBinaryProtocol *tbinaryprotocol;
	ThriftProtocol *tprotocol;
	CassandraClient *client;
	CassandraIf *iface;
};

/*********************/
/* Helper functions: */
/*********************/
void check_gerror(GError **err)
{
	if (*err) {
		//		kp_error("Got GError message: %s\n", (*err)->message);
		g_error_free(*err);
		*err = NULL;
	}
	//else {
	//	kp_debug("No error :)\n");
	//}
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
		return -1;
	}

	for (i = 0; i < len; i++) {
		(*string)[i] = (char)((bytes->data)[i]);
	}
	(*string)[len] = '\0';
	//kp_debug("converted byte array to string: %s\n", *string);

	return 0;
}

/***********************/
/* Internal functions: */
/***********************/
/* To set up exception pointers for these functions: DON'T allocate the
 * exception now using g_object_new(TYPE_INVALID_REQUEST_EXCEPTION),
 * etc.; if you do this and then pass it to the cassandra_if_*() functions,
 * they all will fail!
 * I have determined empirically that the proper way to handle these
 * exceptions and the GError is to first make sure that ALL of them
 * are set to NULL before calling the cassandra_if_*()
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

/* Allocates a new cassandra_node struct and initializes its fields.
 * Returns: pointer to new cassandra_node, or NULL on error.
 */
cassandra_node *cassandra_node_alloc(const char *hostname, unsigned int port)
{
	size_t len;
	cassandra_node *node;
	
	node = malloc(sizeof(cassandra_node));
	if (node == NULL) {
		kp_error("malloc(cassandra_node) failed\n");
		return NULL;
	}

	node->id = cassandra_node_id_count;
	cassandra_node_id_count++;     // this function isn't thread-safe :)
	len = strlen(hostname) + 1;
	node->hostname = malloc(len);
	if (node->hostname == NULL) {
		kp_error("malloc(hostname) failed\n");
		free(node);
		return NULL;
	}
	strncpy(node->hostname, hostname, len);
	node->port = port;
	node->tsocket = NULL;
	node->tframedtransport = NULL;
	node->ttransport = NULL;
	node->tbinaryprotocol = NULL;
	node->tprotocol = NULL;
	node->client = NULL;
	node->iface = NULL;

	return node;
}

/* Frees a cassandra_node struct.
 */
void cassandra_node_free(cassandra_node *node)
{
	if (!node) {
		kp_error("node is NULL!\n");
		return;
	}
	if (node->hostname) {
		free(node->hostname);
	}
	if (node->client) {  //iface is a cast of this
		g_object_unref(node->client);
	}
	if (node->tbinaryprotocol) {  //tprotocol is a cast of this
		g_object_unref(node->tbinaryprotocol);
	}
	if (node->tframedtransport) {  //ttransport is a cast of this
		g_object_unref(node->tframedtransport);
	}
	if (node->tsocket) {
		g_object_unref(node->tsocket);
	}
	free(node);
}

/* Prints the Cassandra node's keyspace information.
 * Returns: 0 on success, -1 on error.
 */
int cassandra_describe_keyspaces(cassandra_node *node)
{
	int i;
	gboolean retbool;
	GPtrArray *retarray;
	GError *err;
	InvalidRequestException *invalidreq_except;
	KsDef *ksdef;  //Keyspace definition
	
	err = NULL;
	invalidreq_except = NULL;
	retarray = g_ptr_array_new();

	retbool = cassandra_if_describe_keyspaces(node->iface, &retarray,
			&invalidreq_except, &err);
	check_gerror(&err);
	if (invalidreq_except) {
		kp_error("cassandra_if_describe_keyspaces() returned "
				"InvalidRequestException: %s\n", invalidreq_except->why);
		g_object_unref(invalidreq_except);
		retbool = FALSE;  //make sure to hit error case that comes next
	}
	if (retbool != TRUE) {
		kp_error("cassandra_if_describe_keyspaces() returned error\n");
		g_ptr_array_free(retarray, TRUE);
		return -1;
	}

	for (i = 0; i < retarray->len; i++) {
		ksdef = (KsDef *)g_ptr_array_index(retarray, i);
		if (ksdef) {
			kp_debug("Keyspace name=[%s]\n", ksdef->name);
		} else {
			kp_debug("Keyspace pointer is null!?\n");
		}
	}
	g_ptr_array_free(retarray, TRUE);

	return 0;
}

/* Creates a keyspace with the specified name.
 * Returns: 0 on success, -1 on error.
 */
int cassandra_create_keyspace(cassandra_node *node, const char *keyspace_name)
{
	gboolean retbool;
	GError *err;
	InvalidRequestException *invalidreq_except;
	
	err = NULL;
	invalidreq_except = NULL;

	retbool = cassandra_if_set_keyspace(node->iface, keyspace_name,
			&invalidreq_except, &err);
	check_gerror(&err);
	if (invalidreq_except) {
		kp_error("cassandra_if_set_keyspace(%s) returned "
				"InvalidRequestException: %s\n", keyspace_name,
				invalidreq_except->why);
		g_object_unref(invalidreq_except);
		retbool = FALSE;  //make sure to hit error case that comes next
	}
	if (retbool != TRUE) {
		kp_error("cassandra_if_set_keyspace(%s) returned error\n",
				keyspace_name);
		return -1;
	}
	kp_debug("cassandra_if_set_keyspace(%s) returned success\n",
			keyspace_name);
	return 0;

}

/* Connects to the specified keyspace.
 * Returns: 0 on success, -1 on error, -2 if the keyspace name was not
 * found.
 */
int cassandra_set_keyspace(cassandra_node *node, const char *keyspace_name)
{
	gboolean retbool;
	GError *err;
	InvalidRequestException *invalidreq_except;
	
	err = NULL;
	invalidreq_except = NULL;

	retbool = cassandra_if_set_keyspace(node->iface, keyspace_name,
			&invalidreq_except, &err);
	check_gerror(&err);
	if (invalidreq_except) {
		kp_error("cassandra_if_set_keyspace(%s) returned "
				"InvalidRequestException: %s\n", keyspace_name,
				invalidreq_except->why);
		g_object_unref(invalidreq_except);
		retbool = FALSE;  //make sure to hit error case that comes next
	}
	if (retbool != TRUE) {
		kp_error("cassandra_if_set_keyspace(%s) returned error\n",
				keyspace_name);
		return -1;
	}
	kp_debug("cassandra_if_set_keyspace(%s) returned success\n",
			keyspace_name);
	return 0;
}

/* Drops the column family with the specified name.
 * Returns: 0 on success, -1 on error, -2 if the column name was not
 * found.
 */
int cassandra_drop_column_family(cassandra_node *node, const char *col_fam_name)
{
	int retval;
	gboolean retbool;
	gchar *retstring;
	GError *err;
	InvalidRequestException *invalidreq_except;
	SchemaDisagreementException *schemadisagree_except;
	const char *not_found_message = "CF is not defined in that keyspace";

	retval = -1;
	err = NULL;
	invalidreq_except = NULL;
	schemadisagree_except = NULL;
	retstring = malloc(RETSTRING_LENGTH);

	retbool = cassandra_if_system_drop_column_family(node->iface, &retstring,
			col_fam_name, &invalidreq_except, &schemadisagree_except, &err);
	if (invalidreq_except) {
		if (strncmp(invalidreq_except->why, not_found_message,
					strlen(not_found_message)) == 0) {
			/* Special case for when column to drop is not found: */
			kp_debug("did not find column to drop: %s\n", col_fam_name);
			kp_debug("returning -2\n");
			retval = -2;
		} else {
			kp_error("cassandra_if_system_drop_column_family(%s) returned "
					"InvalidRequestException: %s\n", col_fam_name,
					invalidreq_except->why);
		}
		g_object_unref(invalidreq_except);
		retbool = FALSE;  //make sure to hit error case that comes next
	}
	if (retval != -2) {
		check_gerror(&err);
	}
	if (schemadisagree_except) {
		kp_error("cassandra_if_system_drop_column_family(%s) returned "
				"SchemaDisagreementException!\n", col_fam_name);
		g_object_unref(schemadisagree_except);
		retbool = FALSE;  //make sure to hit error case that comes next
	}
	if (retbool != TRUE) {
		if (retval != -2) {
			kp_error("cassandra_if_system_drop_column_family(%s) returned error\n",
					col_fam_name);
		}
		free(retstring);
		return retval;
	}

	kp_debug("cassandra_if_system_drop_column_family(%s) succeeded, "
			"returned new schema version ID=%s\n",
			col_fam_name, retstring);
	free(retstring);
	return 0;
}

/* Adds a column family with the specified name. Default/standard values
 * are used for the rest of the parameters, for now.
 * Returns: 0 on success, -1 on error.
 */
int cassandra_add_column_family(cassandra_node *node, char *keyspace,
		char *col_fam_name)
{
	gboolean retbool;
	gchar *retstring;
	GError *err;
	InvalidRequestException *invalidreq_except;
	SchemaDisagreementException *schemadisagree_except;
	CfDef *cf_def;
	char *comparator_type;
	char *default_validation_class;
	char *key_validation_class;
	char *column_type;

	err = NULL;
	invalidreq_except = NULL;
	schemadisagree_except = NULL;
	comparator_type = CASSANDRA_COMPARATOR_TYPE;
	default_validation_class = CASSANDRA_DEFAULT_VALIDATION_CLASS;
	key_validation_class = CASSANDRA_KEY_VALIDATION_CLASS;
	column_type = "Standard";
	  /* Cassandra source java/org/apache/cassandra/db/ColumnFamilyType.java:
	   * choices are "Standard" or "Super." (this is not UTF8Type vs.
	   * BytesType vs. whatever)
	   */

	/* CfDef has a bazillion fields
	 * (http://wiki.apache.org/cassandra/API#CfDef); the code below matches
	 * the default values that are used for a simple ColumnFamily created
	 * from Cassandra CLI (run "describe Keyspace1;" to see these values).
	 */
	cf_def = (CfDef *)g_object_new(TYPE_CF_DEF, NULL);
	if (cf_def == NULL) {
		kp_error("g_object_new(TYPE_CF_DEF) failed\n");
		return -1;
	}
	cf_def->keyspace = keyspace;  //arg to this function
	cf_def->name = col_fam_name;  //arg to this function
	cf_def->column_type = column_type;
	cf_def->__isset_column_type = TRUE;
	cf_def->comparator_type = comparator_type;
	cf_def->__isset_comparator_type = TRUE;
	cf_def->__isset_subcomparator_type = FALSE;
	cf_def->__isset_comment = FALSE;
	cf_def->__isset_row_cache_size = FALSE;
	cf_def->__isset_key_cache_size = FALSE;
	cf_def->__isset_read_repair_chance = FALSE;
	cf_def->__isset_column_metadata = FALSE;
	cf_def->__isset_gc_grace_seconds = FALSE;
	cf_def->default_validation_class = default_validation_class;
	cf_def->__isset_default_validation_class = TRUE;
	cf_def->__isset_id = FALSE;
	cf_def->__isset_min_compaction_threshold = FALSE;
	cf_def->__isset_max_compaction_threshold = FALSE;
	cf_def->__isset_row_cache_save_period_in_seconds = FALSE;
	cf_def->__isset_key_cache_save_period_in_seconds = FALSE;
	cf_def->__isset_replicate_on_write = FALSE;
	cf_def->__isset_merge_shards_chance = FALSE;
	cf_def->key_validation_class = key_validation_class;
	cf_def->__isset_key_validation_class = TRUE;
	cf_def->__isset_row_cache_provider = FALSE;
	cf_def->__isset_key_alias = FALSE;
	cf_def->__isset_compaction_strategy = FALSE;
	cf_def->__isset_compaction_strategy_options = FALSE;
	cf_def->__isset_row_cache_keys_to_save = FALSE;
	cf_def->__isset_compression_options = FALSE;
	retstring = malloc(RETSTRING_LENGTH);

	retbool = cassandra_if_system_add_column_family(node->iface, &retstring,
			cf_def, &invalidreq_except, &schemadisagree_except, &err);
	check_gerror(&err);
	if (invalidreq_except) {
		/* If column family with this name already exists, will get error,
		 * "why" will be: "<col_fam_name> already exists in keyspace Keyspace1"
		 */
		kp_error("cassandra_if_system_add_column_family(%s) returned "
				"InvalidRequestException: %s\n", col_fam_name,
				invalidreq_except->why);
		g_object_unref(invalidreq_except);
		retbool = FALSE;  //make sure to hit error case that comes next
	}
	if (schemadisagree_except) {
		kp_error("cassandra_if_system_add_column_family(%s) returned "
				"SchemaDisagreementException!\n", col_fam_name);
		g_object_unref(schemadisagree_except);
		retbool = FALSE;  //make sure to hit error case that comes next
	}
	if (retbool != TRUE) {
		kp_error("cassandra_if_system_add_column_family(%s) returned error\n",
				col_fam_name);
		free(retstring);
		g_object_unref(cf_def);
		return -1;
	}

	/* IMPORTANT: note that after cassandra_if_system_add_column_family()
	 * successfully returns, "list" or other commands on the new CF may
	 * return "<column_name> not found in current keyspace." This can be
	 * solved by restarting the Cassandra instance or the CLI.
	 */
	kp_debug("cassandra_if_system_add_column_family(%s) succeeded, "
			"returned new schema version ID=%s\n",
			col_fam_name, retstring);
	free(retstring);
	g_object_unref(cf_def);
	return 0;
}

/***********************/
/* External functions: */
/***********************/
int cassandra_start(cassandra_node **node, unsigned int port)
{
	int ret;
	gboolean retbool;
	gchar *retstring;
	GError *err = NULL;

	/* Initialize GObject and GType stuff: */
	g_type_init ();

	*node = cassandra_node_alloc(CASSANDRA_HOSTNAME, port);
	if (*node == NULL) {
		kp_error("cassandra_node_alloc() failed\n");
		return -1;
	}

	/* Use g_object_new() (from GLib) to allocate a new instance of a
	 * ThriftSocket object. Private members of the object are specified
	 * as parameters here. ThriftSocket is part of the Thrift library, not
	 * Cassandra; in the Thrift source, the definition of ThriftSocket
	 * (and its members) can be seen in src/transport/thrift_socket.h.
	 */
	kp_debug("Attempting to connect to server %s:%u\n",
			(*node)->hostname, (*node)->port);
	(*node)->tsocket = (ThriftSocket *)g_object_new(THRIFT_TYPE_SOCKET,
			"hostname", (*node)->hostname, "port", (*node)->port, NULL);
	if ((*node)->tsocket == NULL) {
		kp_error("g_object_new(THRIFT_TYPE_SOCKET) failed\n");
		cassandra_node_free(*node);
		return -1;
	}

	/* See test/testframedtransport.c in Thrift source for some sample
	 * code for TFramedTransport client. Allocate a new
	 * ThriftFramedTransport object: its properties (e.g. w_buf_size) are
	 * defined in include/thrift/transport/thrift_framed_transport.h. */
	(*node)->tframedtransport = (ThriftFramedTransport *)g_object_new(THRIFT_TYPE_FRAMED_TRANSPORT,
			"transport", THRIFT_TRANSPORT ((*node)->tsocket),
			"w_buf_size", 4, NULL);
	  /* Note: THRIFT_TRANSPORT(tsocket) is a CAST from ThriftSocket to
	   *   ThriftTransport!!! */
	if ((*node)->tframedtransport == NULL) {
		kp_error("g_object_new(THRIFT_TYPE_FRAMED_TRANSPORT) failed\n");
		cassandra_node_free(*node);
		return -1;
	}

	/* Cast from ThriftFramedTransport to ThriftTransport.
	 * THRIFT_TRANSPORT is defined in Thrift library in
	 * include/thrift/transport/thrift_transport.h. */
	(*node)->ttransport = THRIFT_TRANSPORT((*node)->tframedtransport);

	/* See test/testbinaryprotocol.c in Thrift source for some sample code
	 * for ThriftBinaryProtocol. At lines 409 and 501, a
	 * THRIFT_TYPE_BINARY_PROTOCOL is used/created with a ThriftTransport,
	 * rather than a ThriftSocket, which is what we're trying to do here
	 * (we're also trying to imitate
	 * http://wiki.apache.org/cassandra/ThriftExamples#C.2B-.2B-) */
	(*node)->tbinaryprotocol = (ThriftBinaryProtocol *)g_object_new(THRIFT_TYPE_BINARY_PROTOCOL,
			"transport", (*node)->ttransport, NULL);
	  /* Note: the "transport" property of ThriftBinaryProtocol is
	   * specified for its (abstract) parent class, ThriftProtocol, in
	   * include/thrift/protocol/thrift_protocol.h (L86). It is expected
	   * to be of type ThriftTransport. */
	if ((*node)->tbinaryprotocol == NULL) {
		kp_error("g_object_new(THRIFT_TYPE_BINARY_PROTOCOL) failed\n");
		cassandra_node_free(*node);
		return -1;
	}

	/* Cast from ThriftBinaryProtocol to ThriftProtocol: */
	(*node)->tprotocol = THRIFT_PROTOCOL((*node)->tbinaryprotocol);

	/* Create Cassandra client and interface objects: */
	(*node)->client = (CassandraClient *)g_object_new(TYPE_CASSANDRA_CLIENT,
			"input_protocol", (*node)->tprotocol, "output_protocol",
			(*node)->tprotocol, NULL);
	  /* Definition of struct _CassandraClient in cassandra.h shows that
	   * input_protocol and output_protocol should be ThriftProtocols! */
	if ((*node)->client == NULL) {
		kp_error("g_object_new(TYPE_CASSANDRA_CLIENT) failed\n");
		cassandra_node_free(*node);
		return -1;
	}
	(*node)->iface = CASSANDRA_IF((*node)->client);

	/* Open Thrift connection to Cassandra: */
	retbool = thrift_transport_open((*node)->ttransport, NULL);
	if (!retbool) {
		kp_error("thrift_transport_open() failed; probably means that "
				"Cassandra instance is not running at %s:%u\n",
				(*node)->hostname, (*node)->port);
		cassandra_node_free(*node);
		return -1;
	}

	/* At this point, use cassandra_if_*() commands (cassandra.h), and
	 * then when finished, call thrift_transport_close(). The
	 * cassandra_if_*() commands lead to the cassandra_client_*()
	 * commands, which wrap around each cassandra_client_send_*() and
	 * cassandra_client_recv_*(). All of these commands are described
	 * here: http://wiki.apache.org/cassandra/API .
	 */

	/* Get the Thrift API version; this isn't necessary at all, but checks
	 * that the connection is working as expected.
	 * http://wiki.apache.org/cassandra/API#describe_version
	 */
	err = NULL;
	retstring = malloc(RETSTRING_LENGTH);  // probably a better way, but good enough
	retbool = cassandra_if_describe_version((*node)->iface, &retstring, &err);
	check_gerror(&err);
	if (!retbool) {
		kp_error("cassandra_if_describe_version() failed; connection is "
				"open, but something else is wrong\n");
		cassandra_node_free(*node);
		return -1;
	}
	kp_print("Successfully connected to Cassandra node at %s:%u, has "
			"Thrift API version %s\n", (*node)->hostname, (*node)->port,
			retstring);
	free(retstring);

	ret = cassandra_describe_keyspaces(*node);
	if (ret != 0) {
		kp_error("cassandra_describe_keyspaces returned error=%d\n", ret);
		return -1;
	}

	// Skip this, for now
	//ret = cassandra_create_keyspace(*node, CASSANDRA_KEYSPACE);
	//if (ret != 0) {
	//		kp_error("cassandra_create_keyspace returned error=%d\n", ret);
	//		return -1;
	//	}
	
	ret = cassandra_set_keyspace(*node, CASSANDRA_KEYSPACE);
	if (ret != 0) {
		kp_error("cassandra_set_keyspace(%s) returned error=%d\n",
				CASSANDRA_KEYSPACE, ret);
		return -1;
	}
	kp_print("set keyspace to %s\n", CASSANDRA_KEYSPACE);

	ret = cassandra_drop_column_family(*node, CASSANDRA_COL_FAM_NAME);
	if (ret != 0 && ret != -2) {
		kp_error("cassandra_drop_column_family(%s) returned error=%d\n",
				CASSANDRA_COL_FAM_NAME, ret);
		return -1;
	}

	/* Unfortunately, if the column family does not already exist in the
	 * keyspace, then we have to create it first; cassandra_if_insert()
	 * won't automatically add it.
	 */
	ret = cassandra_add_column_family(*node, CASSANDRA_KEYSPACE,
			CASSANDRA_COL_FAM_NAME);
	if (ret != 0) {
		kp_error("cassandra_add_column_family(%s) returned error=%d\n",
				CASSANDRA_COL_FAM_NAME, ret);
		return -1;
	}
	kp_print("Added empty column family %s to keyspace\n",
			CASSANDRA_COL_FAM_NAME);

	/* Sleep for a little while here, to allow compaction after dropping
	 * the column family to happen (see log messages that Cassandra prints
	 * after dropping column family), and to allow the added column family
	 * to be initialized... probably unnecessary, but just-in-case for
	 * evaluation purposes.
	 */
	kp_print("Sleeping for a bit to allow column family changes to settle\n");
	sleep(2);

	return 0;
}

int cassandra_worker_start(cassandra_node **node, unsigned int port)
{
	int ret;
	gboolean retbool;
	gchar *retstring;
	GError *err = NULL;

	/* Initialize GObject and GType stuff: */
	g_type_init ();

	*node = cassandra_node_alloc(CASSANDRA_HOSTNAME, port);
	if (*node == NULL) {
		kp_error("cassandra_node_alloc() failed\n");
		return -1;
	}

	/* Use g_object_new() (from GLib) to allocate a new instance of a
	 * ThriftSocket object. Private members of the object are specified
	 * as parameters here. ThriftSocket is part of the Thrift library, not
	 * Cassandra; in the Thrift source, the definition of ThriftSocket
	 * (and its members) can be seen in src/transport/thrift_socket.h.
	 */
	kp_debug("Attempting to connect to server %s:%u\n",
			(*node)->hostname, (*node)->port);
	(*node)->tsocket = (ThriftSocket *)g_object_new(THRIFT_TYPE_SOCKET,
			"hostname", (*node)->hostname, "port", (*node)->port, NULL);
	if ((*node)->tsocket == NULL) {
		kp_error("g_object_new(THRIFT_TYPE_SOCKET) failed\n");
		cassandra_node_free(*node);
		return -1;
	}

	/* See test/testframedtransport.c in Thrift source for some sample
	 * code for TFramedTransport client. Allocate a new
	 * ThriftFramedTransport object: its properties (e.g. w_buf_size) are
	 * defined in include/thrift/transport/thrift_framed_transport.h. */
	(*node)->tframedtransport = (ThriftFramedTransport *)g_object_new(THRIFT_TYPE_FRAMED_TRANSPORT,
			"transport", THRIFT_TRANSPORT ((*node)->tsocket),
			"w_buf_size", 4, NULL);
	  /* Note: THRIFT_TRANSPORT(tsocket) is a CAST from ThriftSocket to
	   *   ThriftTransport!!! */
	if ((*node)->tframedtransport == NULL) {
		kp_error("g_object_new(THRIFT_TYPE_FRAMED_TRANSPORT) failed\n");
		cassandra_node_free(*node);
		return -1;
	}

	/* Cast from ThriftFramedTransport to ThriftTransport.
	 * THRIFT_TRANSPORT is defined in Thrift library in
	 * include/thrift/transport/thrift_transport.h. */
	(*node)->ttransport = THRIFT_TRANSPORT((*node)->tframedtransport);

	/* See test/testbinaryprotocol.c in Thrift source for some sample code
	 * for ThriftBinaryProtocol. At lines 409 and 501, a
	 * THRIFT_TYPE_BINARY_PROTOCOL is used/created with a ThriftTransport,
	 * rather than a ThriftSocket, which is what we're trying to do here
	 * (we're also trying to imitate
	 * http://wiki.apache.org/cassandra/ThriftExamples#C.2B-.2B-) */
	(*node)->tbinaryprotocol = (ThriftBinaryProtocol *)g_object_new(THRIFT_TYPE_BINARY_PROTOCOL,
			"transport", (*node)->ttransport, NULL);
	  /* Note: the "transport" property of ThriftBinaryProtocol is
	   * specified for its (abstract) parent class, ThriftProtocol, in
	   * include/thrift/protocol/thrift_protocol.h (L86). It is expected
	   * to be of type ThriftTransport. */
	if ((*node)->tbinaryprotocol == NULL) {
		kp_error("g_object_new(THRIFT_TYPE_BINARY_PROTOCOL) failed\n");
		cassandra_node_free(*node);
		return -1;
	}

	/* Cast from ThriftBinaryProtocol to ThriftProtocol: */
	(*node)->tprotocol = THRIFT_PROTOCOL((*node)->tbinaryprotocol);

	/* Create Cassandra client and interface objects: */
	(*node)->client = (CassandraClient *)g_object_new(TYPE_CASSANDRA_CLIENT,
			"input_protocol", (*node)->tprotocol, "output_protocol",
			(*node)->tprotocol, NULL);
	  /* Definition of struct _CassandraClient in cassandra.h shows that
	   * input_protocol and output_protocol should be ThriftProtocols! */
	if ((*node)->client == NULL) {
		kp_error("g_object_new(TYPE_CASSANDRA_CLIENT) failed\n");
		cassandra_node_free(*node);
		return -1;
	}
	(*node)->iface = CASSANDRA_IF((*node)->client);

	/* Open Thrift connection to Cassandra: */
	retbool = thrift_transport_open((*node)->ttransport, NULL);
	if (!retbool) {
		kp_error("thrift_transport_open() failed; probably means that "
				"Cassandra instance is not running at %s:%u\n",
				(*node)->hostname, (*node)->port);
		cassandra_node_free(*node);
		return -1;
	}

	/* At this point, use cassandra_if_*() commands (cassandra.h), and
	 * then when finished, call thrift_transport_close(). The
	 * cassandra_if_*() commands lead to the cassandra_client_*()
	 * commands, which wrap around each cassandra_client_send_*() and
	 * cassandra_client_recv_*(). All of these commands are described
	 * here: http://wiki.apache.org/cassandra/API .
	 */

	/* Get the Thrift API version; this isn't necessary at all, but checks
	 * that the connection is working as expected.
	 * http://wiki.apache.org/cassandra/API#describe_version
	 */
	err = NULL;
	retstring = malloc(RETSTRING_LENGTH);  // probably a better way, but good enough
	retbool = cassandra_if_describe_version((*node)->iface, &retstring, &err);
	check_gerror(&err);
	if (!retbool) {
		kp_error("cassandra_if_describe_version() failed; connection is "
				"open, but something else is wrong\n");
		cassandra_node_free(*node);
		return -1;
	}
	kp_print("Successfully connected to Cassandra node at %s:%u, has "
			"Thrift API version %s\n", (*node)->hostname, (*node)->port,
			retstring);
	free(retstring);

	ret = cassandra_describe_keyspaces(*node);
	if (ret != 0) {
		kp_error("cassandra_describe_keyspaces returned error=%d\n", ret);
		return -1;
	}

	// Skip this, for now
	//ret = cassandra_create_keyspace(*node, CASSANDRA_KEYSPACE);
	//if (ret != 0) {
	//	kp_error("cassandra_create_keyspace returned error=%d\n", ret);
	//	return -1;
	//}
	
	ret = cassandra_set_keyspace(*node, CASSANDRA_KEYSPACE);
	if (ret != 0) {
		kp_error("cassandra_set_keyspace(%s) returned error=%d\n",
				CASSANDRA_KEYSPACE, ret);
		return -1;
	}
	kp_print("set keyspace to %s\n", CASSANDRA_KEYSPACE);

	return 0;
}

int cassandra_noop1(cassandra_node *node)
{
	GError *err = NULL;
	gboolean retbool;
	gchar retstring[RETSTRING_LENGTH];
	gchar *retstringptr = retstring;

	retbool = cassandra_if_describe_version(node->iface, &retstringptr, &err);
	if (err) {
		g_error_free(err);
		retbool = FALSE;
	}
	if (!retbool) {
		kp_error("cassandra_if_describe_version() failed\n");
		return -1;
	}
	/* Ignore everything that's returned; this function does as little as
	 * possible, to be as much of a noop as possible.
	 */

	return 0;
}

int cassandra_noop2(cassandra_node *node)
{
	GError *err = NULL;
	gboolean retbool;
	gchar retstring[RETSTRING_LENGTH];
	gchar *retstringptr = retstring;

	retbool = cassandra_if_describe_cluster_name(node->iface, &retstringptr, &err);
	if (err) {
		g_error_free(err);
		retbool = FALSE;
	}
	if (!retbool) {
		kp_error("cassandra_if_describe_cluster_name() failed\n");
		return -1;
	}
	/* Ignore everything that's returned; this function does as little as
	 * possible, to be as much of a noop as possible.
	 */

	return 0;
}

int cassandra_noop3(cassandra_node *node)
{
	GError *err = NULL;
	gboolean retbool;
	gchar retstring[RETSTRING_LENGTH];
	gchar *retstringptr = retstring;

	retbool = cassandra_if_describe_partitioner(node->iface, &retstringptr, &err);
	if (err) {
		g_error_free(err);
		retbool = FALSE;
	}
	if (!retbool) {
		kp_error("cassandra_if_describe_partitioner() failed\n");
		return -1;
	}
	/* Ignore everything that's returned; this function does as little as
	 * possible, to be as much of a noop as possible.
	 */

	return 0;
}

int cassandra_noop4(cassandra_node *node)
{
	GError *err = NULL;
	gboolean retbool;
	gchar retstring[RETSTRING_LENGTH];
	gchar *retstringptr = retstring;

	retbool = cassandra_if_describe_snitch(node->iface, &retstringptr, &err);
	if (err) {
		g_error_free(err);
		retbool = FALSE;
	}
	if (!retbool) {
		kp_error("cassandra_if_describe_snitch() failed\n");
		return -1;
	}
	/* Ignore everything that's returned; this function does as little as
	 * possible, to be as much of a noop as possible.
	 */

	return 0;
}

int cassandra_noop5(cassandra_node *node)
{
	GError *err = NULL;
	gboolean retbool;
	InvalidRequestException *invalidreq_except = NULL;
	GPtrArray *noop_ptr_array;

	/* Really, the caller of this function should probably pre-allocate
	 * this pointer array outside of any timing that is being performed
	 * (and free it after successful call), but this is a pain.
	 */
	noop_ptr_array = g_ptr_array_new();

	retbool = cassandra_if_describe_keyspaces(node->iface, &noop_ptr_array,
			&invalidreq_except, &err);
	if (err) {
		g_error_free(err);
		retbool = FALSE;
	}
	if (invalidreq_except) {
		retbool = FALSE;
		g_object_unref(invalidreq_except);
		retbool = FALSE;  //make sure to hit error case that comes later
	}
	//g_ptr_array_free(noop_ptr_array, TRUE);
	if (!retbool) {
		kp_error("cassandra_if_describe_keyspaces() failed\n");
		return -1;
	}
	/* Ignore everything that's returned; this function does as little as
	 * possible, to be as much of a noop as possible.
	 */

	return 0;
}

int cassandra_worker_stop(cassandra_node *node)
{
	int ret, retval;
	gboolean retbool;

	retval = 0;

	/* Close the Thrift transport: */
	retbool = thrift_transport_close(node->ttransport, NULL);
	if (retbool != TRUE) {
		kp_error("thrift_transport_close() returned error!\n");
		retval = -1;
	}
	kp_debug("thrift_transport_close() succeeded, disconnected from "
			"Cassandra node\n");

	/* Free the cassandra_node struct and all associated GObjects: */
	cassandra_node_free(node);
	kp_debug("freed cassandra_node struct\n");

	return retval;
}

int cassandra_stop(cassandra_node *node)
{
	int ret, retval;
	gboolean retbool;

	retval = 0;

	/* Drop the column family that we added in cassandra_start(): */
	ret = cassandra_drop_column_family(node, CASSANDRA_COL_FAM_NAME);
	if (ret != 0 && ret != -2) {  //ignore col-fam-not-found error
		kp_error("cassandra_drop_column_family(%s) returned error=%d\n",
				CASSANDRA_COL_FAM_NAME, ret);
		retval = -1;
	}

	/* Close the Thrift transport: */
	retbool = thrift_transport_close(node->ttransport, NULL);
	if (retbool != TRUE) {
		kp_error("thrift_transport_close() returned error!\n");
		retval = -1;
	}
	kp_debug("thrift_transport_close() succeeded, disconnected from "
			"Cassandra node\n");

	/* Free the cassandra_node struct and all associated GObjects: */
	cassandra_node_free(node);
	kp_debug("freed cassandra_node struct\n");

	return retval;
}

int cassandra_put(cassandra_node *node, const char *key, const char *value)
{
	int ret;
	gboolean retbool;
	struct timeval tv;
	ColumnParent *col_parent = NULL;
	Column *column = NULL;
	ConsistencyLevel consistency_level_write;
	GByteArray *key_bytearray;
	GByteArray *col_name_bytearray;
	GByteArray *value_bytearray;
	DataModel data_model = CASSANDRA_DATA_MODEL;
	char *col_fam_name = CASSANDRA_COL_FAM_NAME;
	InvalidRequestException *invalidreq_except;
	UnavailableException *unavail_except;
	TimedOutException *timedout_except;
	GError *err;
	
	invalidreq_except = NULL;
	unavail_except = NULL;
	timedout_except = NULL;
	err = NULL;

	/* To match a simple key-value store, seems like there are a few
	 * choices (see
	 * https://sampa.cs.washington.edu/sampa-private/Comparison_to_other_KV_stores
	 * and http://maxgrinev.com/2010/07/09/a-quick-introduction-to-the-cassandra-data-model/):
	 *   Single keyspace
	 *   Single column family
	 *   Either: (data_model)
	 *     - Row keys that match our keys, a single column name, and
	 *       column values that match our values.  (DM_ROWKEYS_USECOLVALS)
	 *     - Row keys that match our keys, column names that match our
	 *       values, column values that are empty  (DM_ROWKEYS_USECOLNAMES)
	 *     - Single row key, column names that match our keys, column
	 *       values that match our values.         (DM_COLNAMEKEYS)
	 *   Additionally, each column has a timestamp as well.
	 * For now, this client uses row keys input by the user, a single
	 * column name (specified as a #define above), and column values input
	 * by the user.
	 */

	/* Set up the column: there are no protected/private
	 * "properties" to set at g_object_new() time, but we set the public
	 * members after the object has been created. See cassandra_types.h:434.
	 */
	col_parent = (ColumnParent *)g_object_new(TYPE_COLUMN_PARENT, NULL);
	if (col_parent == NULL) {
		kp_error("g_object_new(TYPE_COLUMN_PARENT) returned error\n");
		return -1;
	}
	col_parent->column_family = col_fam_name;
	col_parent->__isset_super_column = FALSE;

	/* Set up the key: an array of bytes. To test if the insert worked
	 * using the Cassandra CLI: "get Users['testkey01'];" or "list Users;"
	 * (note that "list Users;" maps to a RangeSliceCommand: see DEBUG
	 * output from Cassandra when running this command).
	 */
	key_bytearray = g_byte_array_new();
	switch(data_model) {
	case DM_ROWKEYS_USECOLVALS:
	case DM_ROWKEYS_USECOLNAMES:
		/* Row key is the key that is passed in as an arg to this
		 * function:
		 */
		convert_string_to_bytearray(key, key_bytearray);
		break;
	case DM_COLNAMEKEYS:
		/* Row key is a single fixed value: */
		convert_string_to_bytearray(CASSANDRA_KEY_NAME, key_bytearray);
		break;
	default:
		kp_die("invalid data_model!\n");
	}
	
	/* Prepare the timestamp: seconds since the epoch. Milliseconds or
	 * microseconds since the epoch would be better, but the stupid
	 * timestamp field is a SIGNED integer (gint64).
	 */
	ret = gettimeofday(&tv, NULL);
	if (ret != 0) {
		kp_error("gettimeofday() returned error=%d\n", ret);
		g_object_unref(col_parent);
		g_byte_array_free(key_bytearray, TRUE);
		return -1;
	}
	kp_debug("timeval: tv_sec=%ld, tv_usec=%ld\n", tv.tv_sec, tv.tv_usec);
	gint64 timestamp = tv.tv_sec;
	kp_debug("calculated timestamp in seconds: %lld\n", timestamp);

	/* Set up the column. Don't set the ttl. See cassandra_types.h:75. */
	column = (Column *)g_object_new(TYPE_COLUMN, NULL);
	if (column == NULL) {
		kp_error("g_object_new(TYPE_COLUMN) failed\n");
		g_object_unref(col_parent);
		g_byte_array_free(key_bytearray, TRUE);
		return -1;
	}
	col_name_bytearray = g_byte_array_new();
	switch(data_model) {
	case DM_ROWKEYS_USECOLVALS:
		/* Column name is a single fixed value; column value is the value
		 * that is passed in as an arg to this function.
		 */
		convert_string_to_bytearray(CASSANDRA_COL_NAME, col_name_bytearray);
		column->name = col_name_bytearray;
		value_bytearray = g_byte_array_new();
		convert_string_to_bytearray(value, value_bytearray);
		column->value = value_bytearray;
		column->__isset_value = TRUE;
		break;
	case DM_ROWKEYS_USECOLNAMES:
		/* Column name is value that is passed in as an arg to this
		 * function; column value is empty.
		 */
		convert_string_to_bytearray(value, col_name_bytearray);
		column->name = col_name_bytearray;
		column->value = NULL;
		column->__isset_value = FALSE;
		break;
	case DM_COLNAMEKEYS:
		/* Column name is key that is passed in as an arg to this function;
		 * column value is value that is passed in as an arg to this
		 * function.
		 */
		convert_string_to_bytearray(key, col_name_bytearray);
		column->name = col_name_bytearray;
		value_bytearray = g_byte_array_new();
		convert_string_to_bytearray(value, value_bytearray);
		column->value = value_bytearray;
		column->__isset_value = TRUE;
		break;
	default:
		kp_die("invalid data_model!\n");
	}
	column->timestamp = timestamp;
	column->__isset_timestamp = TRUE;
	column->__isset_ttl = FALSE;

	/* ConsistencyLevel is an enum, not a struct (cassandra_types.h:28);
	 * see http://wiki.apache.org/cassandra/API#ConsistencyLevel
	 */
	consistency_level_write = CASSANDRA_CONSISTENCY_WRITE;

	/* The actual put() command: */
	retbool = cassandra_if_insert(node->iface, key_bytearray, col_parent,
			column, consistency_level_write, &invalidreq_except,
			&unavail_except, &timedout_except, &err);
	check_gerror(&err);
	if (invalidreq_except) {
		kp_error("cassandra_if_insert(%s, %s) returned "
				"InvalidRequestException: %s\n", key, value,
				invalidreq_except->why);
		g_object_unref(invalidreq_except);
		retbool = FALSE;  //make sure to hit error case that comes later
	}
	if (unavail_except) {
		kp_error("cassandra_if_insert(%s, %s) returned "
				"UnavailableException\n", key, value);
		g_object_unref(unavail_except);
		retbool = FALSE;  //make sure to hit error case that comes later
	}
	if (timedout_except) {
		kp_error("cassandra_if_insert(%s, %s) returned "
				"TimedOutException\n", key, value);
		g_object_unref(timedout_except);
		retbool = FALSE;  //make sure to hit error case that comes later
	}
	g_object_unref(col_parent);
	g_object_unref(column);
	g_byte_array_free(key_bytearray, TRUE);
	g_byte_array_free(col_name_bytearray, TRUE);
	switch(data_model) {
	case DM_ROWKEYS_USECOLVALS:
		g_byte_array_free(value_bytearray, TRUE);
		break;
	case DM_ROWKEYS_USECOLNAMES:
		/* nothing else to free */
		break;
	case DM_COLNAMEKEYS:
		g_byte_array_free(value_bytearray, TRUE);
		break;
	default:
		kp_die("invalid data_model!\n");
	}

	if (retbool != TRUE) {
		kp_error("cassandra_if_insert(%s, %s) returned error\n", key, value);
		return -1;
	}
	kp_debug("Successfully put(%s, %s) in column family %s\n", key, value,
			col_fam_name);

	return 0;
}

int cassandra_put_noop(cassandra_node *node, const char *key, const char *value)
{
	int ret;
	gboolean retbool;
	struct timeval tv;
	ColumnParent *col_parent = NULL;
	Column *column = NULL;
	ConsistencyLevel consistency_level_write;
	GByteArray *key_bytearray;
	GByteArray *col_name_bytearray;
	GByteArray *value_bytearray;
	DataModel data_model = CASSANDRA_DATA_MODEL;
	char *col_fam_name = CASSANDRA_COL_FAM_NAME;
	InvalidRequestException *invalidreq_except;
	UnavailableException *unavail_except;
	TimedOutException *timedout_except;
	GError *err;
	gchar retstring[RETSTRING_LENGTH];
	gchar *retstringptr = retstring;
	
	invalidreq_except = NULL;
	unavail_except = NULL;
	timedout_except = NULL;
	err = NULL;

	/* To match a simple key-value store, seems like there are a few
	 * choices (see
	 * https://sampa.cs.washington.edu/sampa-private/Comparison_to_other_KV_stores
	 * and http://maxgrinev.com/2010/07/09/a-quick-introduction-to-the-cassandra-data-model/):
	 *   Single keyspace
	 *   Single column family
	 *   Either: (data_model)
	 *     - Row keys that match our keys, a single column name, and
	 *       column values that match our values.  (DM_ROWKEYS_USECOLVALS)
	 *     - Row keys that match our keys, column names that match our
	 *       values, column values that are empty  (DM_ROWKEYS_USECOLNAMES)
	 *     - Single row key, column names that match our keys, column
	 *       values that match our values.         (DM_COLNAMEKEYS)
	 *   Additionally, each column has a timestamp as well.
	 * For now, this client uses row keys input by the user, a single
	 * column name (specified as a #define above), and column values input
	 * by the user.
	 */

	/* Set up the column: there are no protected/private
	 * "properties" to set at g_object_new() time, but we set the public
	 * members after the object has been created. See cassandra_types.h:434.
	 */
	col_parent = (ColumnParent *)g_object_new(TYPE_COLUMN_PARENT, NULL);
	if (col_parent == NULL) {
		kp_error("g_object_new(TYPE_COLUMN_PARENT) returned error\n");
		return -1;
	}
	col_parent->column_family = col_fam_name;
	col_parent->__isset_super_column = FALSE;

	/* Set up the key: an array of bytes. To test if the insert worked
	 * using the Cassandra CLI: "get Users['testkey01'];" or "list Users;"
	 * (note that "list Users;" maps to a RangeSliceCommand: see DEBUG
	 * output from Cassandra when running this command).
	 */
	key_bytearray = g_byte_array_new();
	switch(data_model) {
	case DM_ROWKEYS_USECOLVALS:
	case DM_ROWKEYS_USECOLNAMES:
		/* Row key is the key that is passed in as an arg to this
		 * function:
		 */
		convert_string_to_bytearray(key, key_bytearray);
		break;
	case DM_COLNAMEKEYS:
		/* Row key is a single fixed value: */
		convert_string_to_bytearray(CASSANDRA_KEY_NAME, key_bytearray);
		break;
	default:
		kp_die("invalid data_model!\n");
	}
	
	/* Prepare the timestamp: seconds since the epoch. Milliseconds or
	 * microseconds since the epoch would be better, but the stupid
	 * timestamp field is a SIGNED integer (gint64).
	 */
	ret = gettimeofday(&tv, NULL);
	if (ret != 0) {
		kp_error("gettimeofday() returned error=%d\n", ret);
		g_object_unref(col_parent);
		g_byte_array_free(key_bytearray, TRUE);
		return -1;
	}
	kp_debug("timeval: tv_sec=%ld, tv_usec=%ld\n", tv.tv_sec, tv.tv_usec);
	gint64 timestamp = tv.tv_sec;
	kp_debug("calculated timestamp in seconds: %lld\n", timestamp);

	/* Set up the column. Don't set the ttl. See cassandra_types.h:75. */
	column = (Column *)g_object_new(TYPE_COLUMN, NULL);
	if (column == NULL) {
		kp_error("g_object_new(TYPE_COLUMN) failed\n");
		g_object_unref(col_parent);
		g_byte_array_free(key_bytearray, TRUE);
		return -1;
	}
	col_name_bytearray = g_byte_array_new();
	switch(data_model) {
	case DM_ROWKEYS_USECOLVALS:
		/* Column name is a single fixed value; column value is the value
		 * that is passed in as an arg to this function.
		 */
		convert_string_to_bytearray(CASSANDRA_COL_NAME, col_name_bytearray);
		column->name = col_name_bytearray;
		value_bytearray = g_byte_array_new();
		convert_string_to_bytearray(value, value_bytearray);
		column->value = value_bytearray;
		column->__isset_value = TRUE;
		break;
	case DM_ROWKEYS_USECOLNAMES:
		/* Column name is value that is passed in as an arg to this
		 * function; column value is empty.
		 */
		convert_string_to_bytearray(value, col_name_bytearray);
		column->name = col_name_bytearray;
		column->value = NULL;
		column->__isset_value = FALSE;
		break;
	case DM_COLNAMEKEYS:
		/* Column name is key that is passed in as an arg to this function;
		 * column value is value that is passed in as an arg to this
		 * function.
		 */
		convert_string_to_bytearray(key, col_name_bytearray);
		column->name = col_name_bytearray;
		value_bytearray = g_byte_array_new();
		convert_string_to_bytearray(value, value_bytearray);
		column->value = value_bytearray;
		column->__isset_value = TRUE;
		break;
	default:
		kp_die("invalid data_model!\n");
	}
	column->timestamp = timestamp;
	column->__isset_timestamp = TRUE;
	column->__isset_ttl = FALSE;

	/* ConsistencyLevel is an enum, not a struct (cassandra_types.h:28);
	 * see http://wiki.apache.org/cassandra/API#ConsistencyLevel
	 */
	consistency_level_write = CASSANDRA_CONSISTENCY_WRITE;

	/* Replace the actual put() command (cassandra_if_insert()) with a
	 * "noop" command, cassandra_if_describe_cluster_name(). Additionally,
	 * use the data structures that were originally used by
	 * cassandra_if_insert() in a print statement, so that gcc will not
	 * optimize away any work that this function does.
	 */
	kp_print("col_fam_name=%s; key_bytearray[0]=%c; column name[0]=%c; "
			"column value[0]=%c; timestamp=%lld\n",
			col_parent->column_family,
			(key_bytearray->data)[4],
			(column->name->data)[4],
			(column->value->data)[4],
			column->timestamp
			);
	retbool = cassandra_if_describe_cluster_name(node->iface, &retstringptr, &err);
	check_gerror(&err);
	if (invalidreq_except) {
		kp_error("cassandra_if_insert(%s, %s) returned "
				"InvalidRequestException: %s\n", key, value,
				invalidreq_except->why);
		g_object_unref(invalidreq_except);
		retbool = FALSE;  //make sure to hit error case that comes later
	}
	if (unavail_except) {
		kp_error("cassandra_if_insert(%s, %s) returned "
				"UnavailableException\n", key, value);
		g_object_unref(unavail_except);
		retbool = FALSE;  //make sure to hit error case that comes later
	}
	if (timedout_except) {
		kp_error("cassandra_if_insert(%s, %s) returned "
				"TimedOutException\n", key, value);
		g_object_unref(timedout_except);
		retbool = FALSE;  //make sure to hit error case that comes later
	}
	g_object_unref(col_parent);
	g_object_unref(column);
	g_byte_array_free(key_bytearray, TRUE);
	g_byte_array_free(col_name_bytearray, TRUE);
	switch(data_model) {
	case DM_ROWKEYS_USECOLVALS:
		g_byte_array_free(value_bytearray, TRUE);
		break;
	case DM_ROWKEYS_USECOLNAMES:
		/* nothing else to free */
		break;
	case DM_COLNAMEKEYS:
		g_byte_array_free(value_bytearray, TRUE);
		break;
	default:
		kp_die("invalid data_model!\n");
	}

	if (retbool != TRUE) {
		kp_error("cassandra_if_insert(%s, %s) returned error\n", key, value);
		return -1;
	}
	kp_debug("Successfully put(%s, %s) in column family %s\n", key, value,
			col_fam_name);

	return 0;
}

int cassandra_get(cassandra_node *node, const char *key, char **value)
{
	int ret, retval;
	gboolean retbool;
	char *get_name;
	char *get_value;
	ColumnPath *column_path = NULL;
	ColumnOrSuperColumn *col_or_super_col = NULL;
	ConsistencyLevel consistency_level_read;
	GByteArray *key_bytearray;
	GByteArray *col_name_bytearray;
	DataModel data_model = CASSANDRA_DATA_MODEL;
	char *col_fam_name = CASSANDRA_COL_FAM_NAME;
	InvalidRequestException *invalidreq_except;
	UnavailableException *unavail_except;
	TimedOutException *timedout_except;
	NotFoundException *notfound_except;
	GError *err;
	
	invalidreq_except = NULL;
	unavail_except = NULL;
	timedout_except = NULL;
	notfound_except = NULL;
	err = NULL;

	/* Set up the key: an array of bytes. */
	key_bytearray = g_byte_array_new();
	switch(data_model) {
	case DM_ROWKEYS_USECOLVALS:
	case DM_ROWKEYS_USECOLNAMES:
		/* Row key is the key that is passed in as an arg to this
		 * function:
		 */
		convert_string_to_bytearray(key, key_bytearray);
		break;
	case DM_COLNAMEKEYS:
		/* Row key is a single fixed value: */
		convert_string_to_bytearray(CASSANDRA_KEY_NAME, key_bytearray);
		break;
	default:
		kp_die("invalid data_model!\n");
	}

	/* Set up a column path: there are no protected/private
	 * "properties" to set at g_object_new() time, but we set the public
	 * members after the object has been created. ColumnPath is a
	 * combination of ColumnParent and Column.
	 */
	column_path = (ColumnPath *)g_object_new(TYPE_COLUMN_PATH, NULL);
	if (column_path == NULL) {
		kp_error("g_object_new(TYPE_COLUMN_PATH) failed\n");
		g_byte_array_free(key_bytearray, TRUE);
		return -1;
	}
	column_path->column_family = col_fam_name;
	column_path->__isset_super_column = FALSE;
	switch(data_model) {
	case DM_ROWKEYS_USECOLVALS:
		/* Column name is a single fixed value... but this is a get() on
		 * a row key, so is it necessary to specify the column name?
		 * Yes: if no column name is specified, cassandra_if_get() returns
		 * an error, "InvalidRequestException: column parameter is not
		 * optional for standard CF".
		 */
		col_name_bytearray = g_byte_array_new();
		convert_string_to_bytearray(CASSANDRA_COL_NAME, col_name_bytearray);
		column_path->column = col_name_bytearray;
		column_path->__isset_column = TRUE;
		break;
	case DM_ROWKEYS_USECOLNAMES:
		/* We don't know the column name (the column name holds the value
		 * that we want!).
		 */
		column_path->column = NULL;
		column_path->__isset_column = FALSE;
		break;
	case DM_COLNAMEKEYS:
		/* The column name is the key that we want to get() from! (we
		 * specified a single fixed row key). I think that if we were to
		 * not specify a column name here at all, the get() would fetch
		 * ALL of the columns (all of the key-value pairs!) for the
		 * single fixed row key, which would be a lot more work than we
		 * want to perform.
		 */
		col_name_bytearray = g_byte_array_new();
		convert_string_to_bytearray(key, col_name_bytearray);
		column_path->column = col_name_bytearray;
		column_path->__isset_column = TRUE;
		break;
	default:
		kp_die("invalid data_model!\n");
	}

	/* For a Cassandra instance running on just one node, I don't think
	 * the ConsistencyLevel means anything.
	 */
	consistency_level_read = CASSANDRA_CONSISTENCY_READ;

	/* The actual get() command: */
	col_or_super_col = NULL;
	retbool = cassandra_if_get(node->iface, &col_or_super_col, key_bytearray,
			column_path, consistency_level_read, &invalidreq_except,
			&notfound_except, &unavail_except, &timedout_except,
			&err);
	check_gerror(&err);
	retval = -1;
	if (invalidreq_except) {
		kp_error("cassandra_if_get(%s) returned InvalidRequestException: "
				"%s\n", key, invalidreq_except->why);
		g_object_unref(invalidreq_except);
		retbool = FALSE;  //make sure to hit error case that comes later
	}
	if (unavail_except) {
		kp_error("cassandra_if_get(%s) returned UnavailableException\n", key);
		g_object_unref(unavail_except);
		retbool = FALSE;  //make sure to hit error case that comes later
	}
	if (timedout_except) {
		kp_error("cassandra_if_get(%s) returned TimedOutException\n", key);
		g_object_unref(timedout_except);
		retbool = FALSE;  //make sure to hit error case that comes later
	}
	if (notfound_except) {
		//		kp_error("cassandra_if_get(%s) returned NotFoundException\n", key);
		g_object_unref(notfound_except);
		retbool = FALSE;  //make sure to hit error case that comes later
		retval = 1;  //return 1 if key not found!
	}
	g_object_unref(column_path);
	g_byte_array_free(key_bytearray, TRUE);
	switch(data_model) {
	case DM_ROWKEYS_USECOLVALS:
		g_byte_array_free(col_name_bytearray, TRUE);
		break;
	case DM_ROWKEYS_USECOLNAMES:
		/* nothing else to free */
		break;
	case DM_COLNAMEKEYS:
		g_byte_array_free(col_name_bytearray, TRUE);
		break;
	default:
		kp_die("invalid data_model!\n");
	}

	/* Print and free the result
	 * (http://wiki.apache.org/cassandra/API#ColumnOrSuperColumn): */
	if (col_or_super_col) {
		if (col_or_super_col->__isset_column) {
			/* In a Column (http://wiki.apache.org/cassandra/API#Column),
			 * both name and value are required; the value that we want
			 * to return to the user is stored in one of these two fields.
			 */
			ret = convert_bytearray_to_string(col_or_super_col->column->name,
					&get_name);
			if (ret != 0) {
				kp_error("convert_bytearray_to_string(name) returned "
						"error=%d\n", ret);
				retbool = FALSE;
				get_name = NULL;
			} else {
				kp_debug("got column name=%s\n", get_name);
			}
			ret = convert_bytearray_to_string(col_or_super_col->column->value,
					&get_value);
			if (ret != 0) {
				kp_error("convert_bytearray_to_string(value) returned "
						"error=%d\n", ret);
				retbool = FALSE;
				get_value = NULL;
			} else {
				kp_debug("got column value=%s\n", get_value);
			}
			/* convert_bytearray_to_string() allocates a new string and
			 * stores a pointer to it in the second argument that is
			 * passed to it, so we store these pointer values in the
			 * caller's *value:
			 */
			switch(data_model) {
			case DM_ROWKEYS_USECOLVALS:
				/* value that we return comes from the column's value: */
				*value = get_value;
				kp_debug("got column name=%s, expect it to match single "
						"pre-specified column name=%s\n",
						get_name, CASSANDRA_COL_NAME);
				if (get_name) {
					free(get_name);
				}
				break;
			case DM_ROWKEYS_USECOLNAMES:
				/* value that we return comes from the column's name: */
				*value = get_name;
				//todo: convert this warning to debug
				kp_warn("got column value=%s, expect it to be NULL "
						"(right? ? ?)\n", get_value);
				if (get_value) {
					free(get_value);
				}
				break;
			case DM_COLNAMEKEYS:
				/* value that we return comes from the column's value: */
				*value = get_value;
				kp_debug("got column name=%s, expect it to match key=%s\n",
						get_name, key);
				if (get_name) {
					free(get_name);
				}
				break;
			default:
				kp_die("invalid data_model!\n");
			}
		}
		if (col_or_super_col->__isset_super_column) {
			kp_error("unexpectedly got a SuperColumn!\n");
			retbool = FALSE;
		}
		if (!col_or_super_col->__isset_column && !col_or_super_col->__isset_super_column) {
			kp_error("got neither a column nor a super column, this should "
					"never happen\n");
			retbool = FALSE;
		}

		g_object_unref(col_or_super_col);
	}

	if (retbool != TRUE) {
		//		kp_error("cassandra_if_get(%s) returned error (or "
		//				"convert_bytearray_to_string() failed)\n", key);
		*value = NULL;
		return retval;
	}
	kp_debug("Successful get(%s)=%s\n", key, *value);

	return 0;
}

int cassandra_delete(cassandra_node *node, const char *key)
{
	kp_error("not implemented yet\n");
	return -1;
}

/*
 * Editor modelines  -  http://www.wireshark.org/tools/modelines.html
 *
 * Local variables:
 * c-basic-offset: 2
 * tab-width: 2
 * indent-tabs-mode: t
 * End:
 *
 * vi: set noexpandtab:
 * :noTabs=false:
 */
