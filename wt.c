/*-------------------------------------------------------------------------
 * CS448:
 * wt.c
 *	  This code manages relations stored in wiredtiger.
 *
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/wt.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <assert.h>

#include "catalog/catalog.h"
#include "common/relpath.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "utils/memutils.h"
#include "pg_trace.h"
#include "utils/rel.h"
// CS448: add below file for WiredTiger
#include "wiredtiger.h"

/* CS448: helper functions */

/* CS448: get connection to wiredtiger, files related to WT are stored in wt_dir.
 * If connection failed, we return NULL. It is callers job to do NULL pointer
 * check.
 * On successful connection, return pointer of type WT_CONNECTION.
 */
WT_CONNECTION *
get_wtconn( const char* wt_dir ) {
   assert( wt_dir );

   WT_CONNECTION *conn;
   int ret;

   /* Open a connection to the database, creating it if necessary. */
   if ( ( ret = wiredtiger_open( wt_dir, NULL, "create", &conn)) != 0 ) {
       fprintf( stderr, "Error connecting to %s: %s\n",
                wt_dir == NULL ? "." : wt_dir, wiredtiger_strerror( ret ) );
       return NULL;
   }
   return conn;
}

/* CS448: Initiate session to wiredtiger, connection details and directory to WT are
 * passed as arguments.
 * If we fail to establish session, we return NULL. It is callers job to do NULL
 * pointer check.
 * On success, return pointer of type WT_SESSION.
 */
WT_SESSION *
get_wtsession( WT_CONNECTION * conn, const char* wt_dir ) {
    assert( conn );
    assert( wt_dir );

    WT_SESSION *session;
    int ret;
    if ( ( ret = conn->open_session( conn, NULL, NULL, &session ) ) != 0 ) {
        fprintf( stderr, "Error opening a session on %s: %s\n",
                 wt_dir == NULL ? "." : wt_dir, wiredtiger_strerror( ret ) );
        return NULL;
    }
    return session;
}

/*  CS448: Close the connection */
void
close_wtconn( WT_CONNECTION * conn, const char* wt_dir ) {
    assert( conn );
    assert( wt_dir );

    int ret;
    if ( ( ret = conn->close( conn, NULL ) ) != 0 ) {
        fprintf( stderr, "Error closing %s: %s\n",
                 wt_dir == NULL ? "." : wt_dir, wiredtiger_strerror( ret ) );
    }
}

/* Location to store wiredtiger files. */
#define WT_DATA_DIR "/tmp/hspabla/"
#define WT_TABLE_LIST "/tmp/hspabla/wt_table_list" // Maintain a separate file that tells which tables exist in WT

typedef char WiredtigerPath[101];


static char*
replaceChar (char *relp)
{
	/* Replace / with _ . */
	int i = 0;
	while (relp[i] != '\0')
	{
		if (relp[i] == '/')
		{
			relp[i] = '_';
		}
		i++;
	}
	return relp;
}

/*
 * Gets a relation name from SMgrRelation and returns it in wiredtigerPath
 */

static void
getWiredtigerPath (WiredtigerPath outBuffer, Relation reln, int blocknum )
{
	char *relp = RelationGetRelationName( ( reln ) );
	relp = replaceChar(relp);
	snprintf(outBuffer, sizeof(WiredtigerPath), "table:%s_%d", relp, blocknum );
}

/*
 * Called by wtexist:
 * Check if the given relation exist in the WT_TABLE_LIST
 * if it does, then there exist a WT relation too, so return true.
 */
static bool
existInFile (WiredtigerPath path)
{
	FILE * fp;
    char * line = NULL;
    size_t len = 0;
    ssize_t read;

    fp = fopen(WT_TABLE_LIST, "r");
    if (fp == NULL) {
        return false;
	}
    WiredtigerPath newLineTerminatedPath;
    snprintf(newLineTerminatedPath, sizeof(WiredtigerPath), "%s\n", path);
    while ((read = getline(&line, &len, fp)) != -1) {
		if(strncmp(line, newLineTerminatedPath, read) == 0) {
			fclose(fp);
			return true;
		}
    }

    fclose(fp);
    if (line)
        free(line);
    return false;
}

/*
 * Called by wtcreate:
 * for each WT table created, add an entry for it in WT_TABLE_LIST
 * These entries are useful later for wtexist
 */
static void listInFile(WiredtigerPath path)
{
	FILE *fp;
	int x;

	fp =fopen(WT_TABLE_LIST, "a+");
	if (!fp) {
		elog(ERROR, "Error writing to %s:\n", WT_TABLE_LIST);
		return;
	}
	fprintf(fp,"%s\n", path);
	fclose(fp);
	return;
}


/* CS448:
 *	wtexists() -- is the relation/fork in wiredtiger ?
 *
 * 	IMPORTANT: this function used to detect wt tables.
 */
bool
wtexists ( Relation reln, int blocknum )
{

	WiredtigerPath path;
	getWiredtigerPath(path, reln, blocknum );
	bool result = existInFile(path);
	return result;
}



/*
 *  ============================================================================================================
 *  DO NOT CHANGE FUNCTIONS ABOVE THIS LINE
 *  ============================================================================================================
 */

/* CS448:
 *	wtcreate() -- Creates a relation in wiredtiger.
 */
void
wtcreate ( Relation reln, int blocknum )
{
	// Add this relation to file WT_TABLE_LIST
	WiredtigerPath path;
	getWiredtigerPath( path, reln, blocknum );
	listInFile(path);

	/*
	 * YOUR CODE HERE
	 * Hint: open a wiredtiger connection, create a session with desired format of keys and values.
	 * Use the session to create a table, where table name is stored in path variable.
	 * close the connection before exit.
	 */

   static const char home[] = WT_DATA_DIR;
   int ret;

   WT_CONNECTION *conn = get_wtconn( home );
   WT_SESSION *session = get_wtsession( conn, home );

   /* Create table */
   ret = session->create( session, path, "key_format=u,value_format=u" );

   close_wtconn( conn, home );
}

/* CS448:
 *	wtread() --  Read the specified block from a relation.
 */
size_t
wtread ( Relation reln, int blocknum, const char * key, const size_t key_len, char *buffer)
{

	/*
	 * YOUR CODE HERE
	 * Hint: open a wiredtiger connection, create a session with desired format of keys and values.
	 * Use the session to open a cursor to your table, where table name is stored in path variable.
	 * Set the appropriate key to the cursor and read the data. Use set_key() and get_value().
	 * close the connection and free wiredtiger resources  before exit
	 */
   WiredtigerPath path;
	getWiredtigerPath( path, reln, blocknum );

   static const char home[] = WT_DATA_DIR;
   WT_CONNECTION *conn = get_wtconn( home );
   WT_SESSION *session = get_wtsession( conn, home );
   WT_CURSOR *cursor;
   int ret;
   size_t ret_size;

   WT_ITEM *wt_key = palloc( sizeof( WT_ITEM ) );
   char* key_data = palloc( key_len );

   memcpy( key_data, key, key_len );
   wt_key->data = key_data;
   wt_key->size = key_len;

   WT_ITEM *val = palloc( sizeof( WT_ITEM ) );

   // open cursor to the table
   ret = session->open_cursor( session, path, NULL, NULL, &cursor );

   // search for val present at block num - which is used as key
   // If we do not find the key, buffer is not updated.
   cursor->set_key( cursor, wt_key );
   if ( ( ret = cursor->search( cursor ) ) == 0 ) {
      ret = cursor->get_value( cursor, val );
      ret_size = val->size;
      memcpy( buffer, val->data, val->size );
   }

   pfree( wt_key );
   pfree( key_data );
   pfree( val );

   close_wtconn( conn, home );
   return ret_size;
}

/* CS448:
 *	wtwrite() --  Write the supplied block at the appropriate location.
 *
 *	Note that we ignore skipFsync since it's used for checkpointing which we
 *	are not going to care about. We synchronously flush to disk anyways.
 */
void
wtwrite ( Relation reln, int blocknum, const char* key,
                         const size_t key_len,
                         const char* val,
                         const size_t val_len )
{

	/*
	 * YOUR CODE HERE
	 * Hint: open a wiredtiger connection, create a session with desired format of keys and values.
	 * Use the session to open a cursor to your table, where table name is stored in path variable.
	 * Set the appropriate key, and value to the cursor and write the data. Use set_key(), set_value(), and insert().
	 * close the connection and free wiredtiger resources  before exit
	 */

   static const char home[] = WT_DATA_DIR;
   WiredtigerPath path;
	getWiredtigerPath( path, reln, blocknum );

   WT_CONNECTION *conn = get_wtconn( home );
   WT_SESSION *session = get_wtsession( conn, home );
   WT_CURSOR *cursor;
   int ret;

   WT_ITEM *wt_key = palloc( sizeof( WT_ITEM ) );
   char* key_data = palloc( key_len );
   memcpy( key_data, key, key_len );
   wt_key->data = key_data;
   wt_key->size = key_len;

   WT_ITEM *wt_val = palloc( sizeof( WT_ITEM ) );
   char* val_data = palloc( val_len );
   memcpy( val_data, val, val_len );
   wt_val->data = val_data;
   wt_val->size = val_len;

   // open cursor and insert key, val
   ret = session->open_cursor( session, path, NULL, NULL, &cursor );
   cursor->set_key( cursor, wt_key );
   cursor->set_value( cursor, wt_val );
   cursor->insert( cursor );

   pfree( wt_key );
   pfree( key_data );
   pfree( wt_val );
   pfree( val_data );

   close_wtconn( conn, home );
}

/*
 * CS448:
 *	wtnblocks() -- Calculate the number of blocks in the
 *					supplied relation.
 */

List*
wtTupleList ( Relation reln, BlockNumber blocknum )
{

	/*
	 * YOUR CODE HERE
	 * Hint: open a wiredtiger connection, create a session with desired format of keys and values.
	 * Use the session to open a cursor to your table, where table name is stored in path variable.
	 * Iterate till the cursor has something to return, and count. Use next().
	 * close the connection and free wiredtiger resources  before exit
	 */

   int item = 0;
   static const char home[] = WT_DATA_DIR;
   List* tuples = NIL;
   struct WTtuple tup;

   WiredtigerPath path;
	getWiredtigerPath( path, reln, blocknum );

   WT_CONNECTION *conn = get_wtconn( home );
   WT_SESSION *session = get_wtsession( conn, home );
   WT_CURSOR *cursor;
   int ret;

   ret = session->open_cursor( session, path, NULL, NULL, &cursor );

   while( ret == 0 ) {
       WT_ITEM *val = palloc( sizeof( WT_ITEM ) );
       ret = cursor->get_value( cursor, val );
       if ( ret == 0 ) {
         struct WTtuple * tup = palloc( sizeof(struct WTtuple) );
         char* raw_item = palloc( val->size );
         memcpy( raw_item, val->data, val->size );
         tup->item = raw_item;
         tup->len = val->size;

         tuples = lappend( tuples, tup );
       }
       ret = cursor->next( cursor );
       pfree( val );
   }
   close_wtconn( conn, home );

   return tuples;
}
