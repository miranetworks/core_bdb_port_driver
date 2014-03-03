#include <db.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>

#include <erl_driver.h>
#include <ei.h>

#ifndef __KVS_BDB_DRV_H__
#define __KVS_BDB_DRV_H__

typedef struct _drv_cfg drv_cfg;

struct _drv_cfg {
    DB_ENV* penv;
    DB*     pdb;
    
    char* data_dir;
    char* db_name;
    char* rep_if;

    char* buffer;

    u_int32_t   db_type;

    u_int32_t   txn_enabled;

    u_int32_t   is_master;   
    u_int32_t   replication_enabled;
    u_int32_t   replication_type;

    u_int32_t   db_open_flags;

    u_int32_t   bulk_get_buffer_size_bytes;
    u_int32_t   page_size_bytes;

};

typedef struct _bdb_drv_t {
  ErlDrvPort port;

  drv_cfg *pcfg;

  unsigned int async_thread_key;

  unsigned long update_counter;
  unsigned long max_updates_before_flush;
  unsigned long last_sync_ms;
  unsigned long max_sync_age_ms;

} bdb_drv_t;

/*
typedef struct _async_set_t {
    bdb_drv_t* pdrv;

    unsigned int key_len;
    char* key_bytes;
    
    unsigned int data_len;
    char* data_bytes;
 
} async_set_t;

typedef struct _async_get_t {
    bdb_drv_t* pdrv;

    unsigned int key_len;
    char* key_bytes;
    
} async_get_t;

typedef struct _async_del_t {
    bdb_drv_t* pdrv;

    unsigned int key_len;
    char* key_bytes;
    
} async_del_t;
*/

static ErlDrvData start(ErlDrvPort port, char* cmd);
static void stop(ErlDrvData handle);
static void outputv(ErlDrvData handle, ErlIOVec *ev);
static void process_open(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void process_set(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void process_get(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void process_del(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void process_count(bdb_drv_t *bdb_drv, ErlIOVec *ev);

static void process_flush(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void async_process_flush(void* pdrv);
static int do_sync(bdb_drv_t* pdrv);

static void process_bulk_get(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void process_compact(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void process_truncate(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void process_unkown(bdb_drv_t *bdb_drv, ErlIOVec *ev);

#if ((DB_VERSION_MAJOR > 4) || ((DB_VERSION_MAJOR >= 4) && (DB_VERSION_MINOR > 4)))
static void process_add_replication_node(bdb_drv_t *bdb_drv, ErlIOVec *ev);
static void event_callback __P((DB_ENV*, u_int32_t, void*));
#endif

static void open_db(bdb_drv_t* pdrv, ErlIOVec *ev);
static void set (u_int32_t key_size, void* praw_key, u_int32_t data_size, void* praw_data, bdb_drv_t *pdrv);
static void get (u_int32_t key_size, void* praw_key, bdb_drv_t *pdrv);
static void del (u_int32_t key_size, void* praw_key, bdb_drv_t *pdrv);
static void bulk_get_btree (u_int32_t offset, u_int32_t count, bdb_drv_t *pdrv);
static void bulk_get_hash (u_int32_t offset, u_int32_t count, bdb_drv_t *pdrv);

#endif

//EOF
