#include "kvs_bdb_drv.h"

static void return_error_tuple(bdb_drv_t* pdrv, char* err_msg) {

    ErlDrvTermData spec[] = {   ERL_DRV_ATOM, driver_mk_atom("error"),
                                ERL_DRV_ATOM, driver_mk_atom(err_msg),
                                ERL_DRV_TUPLE, 2};

#if ((ERL_DRV_EXTENDED_MAJOR_VERSION == 1) || ((ERL_DRV_EXTENDED_MAJOR_VERSION == 2) && (ERL_DRV_EXTENDED_MINOR_VERSION == 0)))
    driver_output_term(pdrv->port, spec, sizeof(spec) / sizeof(spec[0]));
#else
    ErlDrvTermData mkport = driver_mk_port(pdrv->port);
    erl_drv_output_term(mkport, spec, sizeof(spec) / sizeof(spec[0]));
#endif

}

static void return_ok_empty_list(bdb_drv_t* pdrv) {

    ErlDrvTermData empty_spec[] = {

                            ERL_DRV_ATOM, driver_mk_atom("ok"),

                            ERL_DRV_NIL,

                            ERL_DRV_LIST, 1, 

                            ERL_DRV_TUPLE, 2};

#if ((ERL_DRV_EXTENDED_MAJOR_VERSION == 1) || ((ERL_DRV_EXTENDED_MAJOR_VERSION == 2) && (ERL_DRV_EXTENDED_MINOR_VERSION == 0)))
    driver_output_term(pdrv->port, empty_spec, sizeof(empty_spec) / sizeof(empty_spec[0]));
#else
    ErlDrvTermData mkport = driver_mk_port(pdrv->port);
    erl_drv_output_term(mkport, empty_spec, sizeof(empty_spec) / sizeof(empty_spec[0]));
#endif

}

static void return_ok(bdb_drv_t* pdrv) {

    ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok")};

#if ((ERL_DRV_EXTENDED_MAJOR_VERSION == 1) || ((ERL_DRV_EXTENDED_MAJOR_VERSION == 2) && (ERL_DRV_EXTENDED_MINOR_VERSION == 0)))
    driver_output_term(pdrv->port, spec, sizeof(spec) / sizeof(spec[0]));
#else
    ErlDrvTermData mkport = driver_mk_port(pdrv->port);
    erl_drv_output_term(mkport, spec, sizeof(spec) / sizeof(spec[0]));
#endif

}

#ifndef ERL_DRV_EXTENDED_MARKER
// Callback Array
static ErlDrvEntry basic_driver_entry = {
    NULL,                             /* init */
    start,                            /* startup (defined below) */
    stop,                             /* shutdown (defined below) */
    NULL,                             /* output */
    NULL,                             /* ready_input */
    NULL,                             /* ready_output */
    "mira_bdb_port_driver_drv",                        /* the name of the driver */
    NULL,                             /* finish */
    NULL,                             /* handle */
    NULL,                             /* control */
    timeout,                             /* timeout */
    outputv,                          /* outputv (defined below) */
    NULL,                      /* ready_async */
    NULL,                             /* flush */
    NULL,                             /* call */
    NULL                            /* event */

};
#else

// Callback Array
static ErlDrvEntry basic_driver_entry = {
    NULL,                             /* init */
    start,                            /* startup (defined below) */
    stop,                             /* shutdown (defined below) */
    NULL,                             /* output */
    NULL,                             /* ready_input */
    NULL,                             /* ready_output */
    "mira_bdb_port_driver_drv",                        /* the name of the driver */
    NULL,                             /* finish */
    NULL,                             /* handle */
    NULL,                             /* control */
    NULL,                             /* timeout */
    outputv,                          /* outputv (defined below) */
    NULL,                      /* ready_async */
    NULL,                             /* flush */
    NULL,                             /* call */
    NULL,                             /* event */

    ERL_DRV_EXTENDED_MARKER,          /* ERL_DRV_EXTENDED_MARKER */
    ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MAJOR_VERSION */
    ERL_DRV_EXTENDED_MINOR_VERSION,   /* ERL_DRV_EXTENDED_MINOR_VERSION */
    ERL_DRV_FLAG_USE_PORT_LOCKING     /* ERL_DRV_FLAGs */
};

#endif

DRIVER_INIT(basic_driver) {
    return &basic_driver_entry;
}

static ErlDrvData start(ErlDrvPort port, char* cmd) {
    bdb_drv_t* retval = (bdb_drv_t*) driver_alloc(sizeof(bdb_drv_t));

    retval->port = port;

#if ((ERL_DRV_EXTENDED_MAJOR_VERSION == 1) || ((ERL_DRV_EXTENDED_MAJOR_VERSION == 2) && (ERL_DRV_EXTENDED_MINOR_VERSION == 0)))
    retval->async_thread_key = 0;
#else
    retval->async_thread_key = driver_async_port_key(port);
#endif

    retval->pcfg = NULL;

    return (ErlDrvData) retval;

}

static void stop(ErlDrvData handle) {
    bdb_drv_t* pdrv = (bdb_drv_t*) handle;

    if (pdrv->pcfg) {
        if (pdrv->pcfg->pdb) {
            pdrv->pcfg->pdb->sync(pdrv->pcfg->pdb, 0);
            if (pdrv->pcfg->replication_enabled) {
                pdrv->pcfg->penv->rep_sync(pdrv->pcfg->penv, 0);
                pdrv->pcfg->penv->log_flush(pdrv->pcfg->penv, NULL);
            }

            pdrv->pcfg->pdb->close(pdrv->pcfg->pdb, 0);
            pdrv->pcfg->pdb = NULL;
            pdrv->pcfg->penv->close(pdrv->pcfg->penv, 0);
            pdrv->pcfg->penv = NULL;
        }

        if (pdrv->pcfg->buffer) {
            free(pdrv->pcfg->buffer);
            pdrv->pcfg->buffer = NULL;
        }
    }

    driver_free(pdrv);
}


// Handle input from Erlang VM
static void outputv(ErlDrvData handle, ErlIOVec *ev) {
    bdb_drv_t* pdrv = (bdb_drv_t*) handle;

    ErlDrvBinary* data = ev->binv[1];

    int command = data->orig_bytes[0]; // First byte is the command
  
    switch(command) {
    case 'O':
        process_open(pdrv, ev);
        break;

    case 'S':
        process_set(pdrv, ev);
        break;

    case 'G':
        process_get(pdrv, ev);
        break;

    case 'D':
        process_del(pdrv, ev);
        break;

    case 'C':
        process_count(pdrv, ev);
        break;

    case 'F':
        process_flush(pdrv, ev);
        break;

    case 'B':
        process_bulk_get(pdrv, ev);
        break;

    case 'Z':
        process_compact(pdrv, ev);
        break;

    case 'T':
        process_truncate(pdrv, ev);
        break;

#if ((DB_VERSION_MAJOR > 4) || ((DB_VERSION_MAJOR >= 4) && (DB_VERSION_MINOR > 4)))
    case 'R':
        process_add_replication_node(pdrv, ev);
        break;
#endif


    default:
        process_unkown(pdrv, ev);
    }

}

static void process_unkown(bdb_drv_t *bdb_drv, ErlIOVec *ev) {
  // Return {error, unkown_command}
  ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("error"),
               ERL_DRV_ATOM, driver_mk_atom("uknown_command"),
               ERL_DRV_TUPLE, 2};

#if ((ERL_DRV_EXTENDED_MAJOR_VERSION == 1) || ((ERL_DRV_EXTENDED_MAJOR_VERSION == 2) && (ERL_DRV_EXTENDED_MINOR_VERSION == 0)))
    driver_output_term(bdb_drv->port, spec, sizeof(spec) / sizeof(spec[0]));
#else
    ErlDrvTermData mkport = driver_mk_port(bdb_drv->port);
    erl_drv_output_term(mkport, spec, sizeof(spec) / sizeof(spec[0]));
#endif

}


static void process_open( bdb_drv_t* pdrv, ErlIOVec *ev) {

    if (pdrv->pcfg) {
        return_error_tuple(pdrv, "Already opened!");
    } else {
        open_db(pdrv, ev);
    }

}


static void open_db(bdb_drv_t* pdrv, ErlIOVec *ev) {

    drv_cfg* pcfg;

    DB* pdb;
    DB_ENV* penv;
    u_int32_t open_flags, page_size_bytes, cache_size_bytes, bulk_get_buffer_size_bytes;
    int ret;

    ErlDrvBinary* data = ev->binv[1];

    char *bytes = data->orig_bytes;

    int txn_enabled         = bytes[1];
    int db_type             = bytes[2];
    int replication_enabled = bytes[3];

    cache_size_bytes           = (uint32_t) ntohl(* ((uint32_t*) (bytes + 4) ));
    page_size_bytes            = (uint32_t) ntohl(* ((uint32_t*) (bytes + 4 + 4) ));
    bulk_get_buffer_size_bytes = (uint32_t) ntohl(* ((uint32_t*) (bytes + 4 + 4 + 4) ));

    char* lv_start = bytes + 4 + 4 + 4 + 4;

    char *db_name_len_bytes = lv_start;
    char *db_name_bytes     = lv_start + 4;
    uint32_t db_name_length = (uint32_t) ntohl(* ((uint32_t*) db_name_len_bytes) );

    char *data_dir_len_bytes = db_name_bytes + db_name_length;
    char *data_dir_bytes     = data_dir_len_bytes + 4;
    uint32_t data_dir_length = (uint32_t) ntohl(* ((uint32_t*) data_dir_len_bytes) );

    char *replication_if_len_bytes = data_dir_bytes + data_dir_length;
    char *replication_if_bytes     = replication_if_len_bytes + 4;
    uint32_t replication_if_length = (uint32_t) ntohl(* ((uint32_t*) replication_if_len_bytes) );

#if ((DB_VERSION_MAJOR > 4) || ((DB_VERSION_MAJOR >= 4) && (DB_VERSION_MINOR > 4)))
    uint32_t replication_port = (uint32_t) ntohl(* ((uint32_t*) (replication_if_bytes + replication_if_length) ));
#endif

    char replication_type =  ((char*)(replication_if_bytes + replication_if_length + 4))[0];

    pcfg = (drv_cfg*) malloc(sizeof(drv_cfg));

    pcfg->buffer = (char*) malloc(bulk_get_buffer_size_bytes);
    if (pcfg->buffer == NULL) {
        return_error_tuple(pdrv, "Could not allocate memory for operation!");
        return;
    }  

    pcfg->penv = NULL;
    pcfg->pdb  = NULL;

    pcfg->txn_enabled = txn_enabled;

    pcfg->replication_enabled = replication_enabled;

    pcfg->data_dir             = malloc(data_dir_length + 1);
    pcfg->db_name              = malloc(db_name_length + 1);
    pcfg->rep_if               = malloc(replication_if_length + 1);
    pcfg->is_master            = 0;
    pcfg->page_size_bytes      = page_size_bytes;

    pcfg->data_dir[data_dir_length] = 0;
    pcfg->db_name[db_name_length] = 0;
    pcfg->rep_if[replication_if_length] = 0;

    if (db_type == 'B') {
        pcfg->db_type = DB_BTREE;
    } else {
        pcfg->db_type = DB_HASH;
    }

    if (replication_type == 'M') {
        pcfg->replication_type = DB_REP_MASTER;
    } else {
        pcfg->replication_type = DB_REP_CLIENT;
    }

    memcpy(pcfg->data_dir, data_dir_bytes, data_dir_length);
    memcpy(pcfg->db_name, db_name_bytes, db_name_length);
    memcpy(pcfg->rep_if, replication_if_bytes, replication_if_length);

    pcfg->bulk_get_buffer_size_bytes = bulk_get_buffer_size_bytes;

    ret = db_env_create(&penv, 0);
    if (ret != 0) {
        return_error_tuple(pdrv, db_strerror(ret));
        return;
    } 

    penv->app_private = pcfg;

    
    open_flags = DB_CREATE | DB_INIT_MPOOL;

    if (pcfg->txn_enabled) {

        open_flags |= DB_INIT_LOCK | DB_THREAD | DB_INIT_TXN | DB_INIT_LOG | DB_REGISTER | DB_RECOVER;

        if (pcfg->replication_enabled) {
            open_flags |= DB_INIT_REP;
        }

    }

    //penv->set_msgcall(penv, (FILE*)stderr);
    //penv->set_verbose(penv, DB_VERB_DEADLOCK | DB_VERB_RECOVERY, 1);

    ret = penv->set_cachesize(penv, 0, cache_size_bytes, 1);
    if (ret != 0) {
        return_error_tuple(pdrv, db_strerror(ret));
        return;
    }

    ret = penv->open(penv, pcfg->data_dir, open_flags, 0);
    if (ret != 0) {
        return_error_tuple(pdrv, db_strerror(ret));
        return;
    }

//Replication stuff ....

    if (pcfg->replication_enabled) {

#if ((DB_VERSION_MAJOR > 4) || ((DB_VERSION_MAJOR >= 4) && (DB_VERSION_MINOR > 4)))

        penv->set_event_notify(penv, event_callback);

        if ((ret = penv->repmgr_set_local_site(penv, pcfg->rep_if, replication_port, 0)) != 0) {
            return_error_tuple(pdrv, db_strerror(ret));
            return;
        }

        if (pcfg->replication_type == DB_REP_MASTER) {
            penv->rep_set_priority(penv, 100);
        } else {
            penv->rep_set_priority(penv, 0);
        }

        ret = penv->repmgr_set_ack_policy(penv, DB_REPMGR_ACKS_NONE);
        if (ret != 0) {
            return_error_tuple(pdrv, db_strerror(ret));
            return;
        }

        ret = penv->repmgr_start(penv, 10, pcfg->replication_type);
        if (ret != 0) {
            return_error_tuple(pdrv, db_strerror(ret));
            return;
        }
#else

        return_error_tuple(pdrv, "Replication not supported in this version of Berkeley DB");
        return;

#endif

    } else {

        //Only open the table if not in replication mode.... for rep mode the table will be opened in event handler ...

        ret = db_create(&pdb, penv, 0);
        if (ret != 0) {
            return_error_tuple(pdrv, db_strerror(ret));
            return;
        }

        if (pcfg->db_type == DB_BTREE) {

            ret = pdb->set_flags(pdb, DB_RECNUM);
            if (ret != 0) {
                return_error_tuple(pdrv, db_strerror(ret));
                return;
            }
        }

        ret = pdb->set_pagesize(pdb, page_size_bytes);
        if (ret != 0) {
            return_error_tuple(pdrv, db_strerror(ret));
            return;
        }

        pdb->set_errpfx(pdb, pcfg->db_name);

        pcfg->db_open_flags = DB_CREATE;
 
        if (pcfg->txn_enabled) {
            pcfg->db_open_flags |= DB_THREAD; 
        }

        if ((ret = pdb->open(pdb, NULL, pcfg->db_name, pcfg->db_name, pcfg->db_type, pcfg->db_open_flags, 0)) != 0) {
            return_error_tuple(pdrv, db_strerror(ret));
            return;
        }

        pcfg->pdb  = pdb;
    }

    pcfg->penv = penv;

    pdrv->pcfg = pcfg;

    return_ok(pdrv);

    return;

}

static void process_set( bdb_drv_t* pdrv, ErlIOVec *ev) {
    ErlDrvBinary* data = ev->binv[1];

    char *bytes = data->orig_bytes;

    char *key_len_bytes = bytes + 1;
    char *key_bytes     = bytes + 1 + 4;
    unsigned int key_length = (unsigned int) ntohl(* ((uint32_t*) key_len_bytes) );

    char *value_len_bytes = bytes + 1 + 4 + key_length;
    char *value_bytes     = bytes + 1 + 4 + key_length + 4;
    unsigned int value_length = (unsigned int) ntohl(* ((uint32_t*) value_len_bytes) );

    if (pdrv->pcfg) {
        if (pdrv->pcfg->pdb == NULL) {
            return_error_tuple(pdrv, "Database not opened!");
            return;
        }
        set (key_length, key_bytes, value_length, value_bytes, pdrv);
    } else {
        return_error_tuple(pdrv, "Database not opened!");
    }

}

static void process_get( bdb_drv_t* pdrv, ErlIOVec *ev) {
    ErlDrvBinary* data = ev->binv[1];

    char *bytes = data->orig_bytes;

    char *key_len_bytes = bytes + 1;
    char *key_bytes     = bytes + 1 + 4;
    unsigned int key_length = (unsigned int) ntohl(* ((uint32_t*) key_len_bytes) );

    if (pdrv->pcfg) {
        if (pdrv->pcfg->pdb == NULL) {
            return_error_tuple(pdrv, "Database not opened!");
            return;
        }
        get (key_length, key_bytes, pdrv);
    } else {
        return_error_tuple(pdrv, "Database not opened!");
    }

}

static void process_del( bdb_drv_t* pdrv, ErlIOVec *ev) {
    ErlDrvBinary* data = ev->binv[1];

    char *bytes = data->orig_bytes;

    char *key_len_bytes = bytes + 1;
    char *key_bytes     = bytes + 1 + 4;
    unsigned int key_length = (unsigned int) ntohl(* ((uint32_t*) key_len_bytes) );

    if (pdrv->pcfg) {
        if (pdrv->pcfg->pdb == NULL) {
            return_error_tuple(pdrv, "Database not opened!");
            return;
        }
        del (key_length, key_bytes, pdrv);
    } else {
        return_error_tuple(pdrv, "Database not opened!");
    }

}

static void process_count( bdb_drv_t* pdrv, ErlIOVec *ev) {

    DB_BTREE_STAT* btree_stats;
    DB_HASH_STAT* hash_stats;

    int ret, count;

    count = 0;

    if (pdrv->pcfg == NULL) {
        return_error_tuple(pdrv, "Database not opened!");
        return;
    }

    if (pdrv->pcfg->pdb == NULL) {
        return_error_tuple(pdrv, "Database not opened!");
        return;
    }

    while (1) {

        if (pdrv->pcfg->db_type == DB_BTREE) {

            ret = pdrv->pcfg->pdb->stat(pdrv->pcfg->pdb, NULL, &btree_stats, DB_FAST_STAT);

            if (ret != 0) {

                if (count > 2) {
                    return_error_tuple(pdrv, db_strerror(ret));
                    break;
                } else {
                    count = count + 1;
                    usleep(1000);
                }

            } else {

                ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok"),

                    ERL_DRV_INT, btree_stats->bt_nkeys,

                    ERL_DRV_TUPLE, 2};

#if ((ERL_DRV_EXTENDED_MAJOR_VERSION == 1) || ((ERL_DRV_EXTENDED_MAJOR_VERSION == 2) && (ERL_DRV_EXTENDED_MINOR_VERSION == 0)))
                driver_output_term(pdrv->port, spec, sizeof(spec) / sizeof(spec[0]));
#else
                ErlDrvTermData mkport = driver_mk_port(pdrv->port);
                erl_drv_output_term(mkport, spec, sizeof(spec) / sizeof(spec[0]));
#endif

                free(btree_stats);

                break;
            }


        } else {

            ret = pdrv->pcfg->pdb->stat(pdrv->pcfg->pdb, NULL, &hash_stats, 0);

            if (ret != 0) {

                if (count > 2) {
                    return_error_tuple(pdrv, db_strerror(ret));
                    break;
                } else {
                    count = count + 1;
                    usleep(1000);
                }

            } else {

                ErlDrvTermData spec[] = {ERL_DRV_ATOM, driver_mk_atom("ok"),

                    ERL_DRV_INT, hash_stats->hash_nkeys,

                    ERL_DRV_TUPLE, 2};

#if ((ERL_DRV_EXTENDED_MAJOR_VERSION == 1) || ((ERL_DRV_EXTENDED_MAJOR_VERSION == 2) && (ERL_DRV_EXTENDED_MINOR_VERSION == 0)))
                driver_output_term(pdrv->port, spec, sizeof(spec) / sizeof(spec[0]));
#else
                ErlDrvTermData mkport = driver_mk_port(pdrv->port);
                erl_drv_output_term(mkport, spec, sizeof(spec) / sizeof(spec[0]));
#endif

                free(hash_stats);

                break;
            }


        }

    }

    return;

}

static void async_process_flush_done(void* pdata);

static void process_flush( bdb_drv_t* pdrv, ErlIOVec *ev) {

    if (pdrv->pcfg == NULL) {
        return_error_tuple(pdrv, "Database not opened!");
        return;
    }

    if (pdrv->pcfg->pdb == NULL) {
        return_error_tuple(pdrv, "Database not opened!");
        return;
    }

    driver_async(pdrv->port, &pdrv->async_thread_key, async_process_flush, (void*) pdrv, async_process_flush_done);

 }

static void async_process_flush_done(void* pdata) {}

static void async_process_flush(void* pdata) {

    bdb_drv_t* pdrv = (bdb_drv_t*) pdata;

    int ret;

    if ((ret = do_sync(pdrv)) == 0) {
        return_ok(pdrv);
    } else {
        return_error_tuple(pdrv, db_strerror(ret));
    }

    return;
}

static int do_sync(bdb_drv_t* pdrv) {
    int ret = 0;

    if ((ret = pdrv->pcfg->pdb->sync(pdrv->pcfg->pdb, 0)) == 0) {
        if (pdrv->pcfg->replication_enabled) {
            if ((ret = pdrv->pcfg->penv->rep_sync(pdrv->pcfg->penv, 0)) == 0) {
                ret = pdrv->pcfg->penv->log_flush(pdrv->pcfg->penv, NULL);
            }
        }
    }
    return ret;
}

static void process_compact( bdb_drv_t* pdrv, ErlIOVec *ev) {

    int ret;

    if (pdrv->pcfg == NULL) {
        return_error_tuple(pdrv, "Database not opened!");
        return;
    }

    if (pdrv->pcfg->pdb == NULL) {
        return_error_tuple(pdrv, "Database not opened!");
        return;
    }

    ret = pdrv->pcfg->pdb->compact(pdrv->pcfg->pdb, NULL, NULL, NULL, NULL, 0, NULL);

    if (ret) {
        return_error_tuple(pdrv, db_strerror(ret));
    } else {
        return_ok(pdrv);
    }

    return;

}

static void process_truncate( bdb_drv_t* pdrv, ErlIOVec *ev) {

    u_int32_t count;
    int ret;

    if (pdrv->pcfg == NULL) {
        return_error_tuple(pdrv, "Database not opened!");
        return;
    }

    if (pdrv->pcfg->pdb == NULL) {
        return_error_tuple(pdrv, "Database not opened!");
        return;
    }

    ret = pdrv->pcfg->pdb->truncate(pdrv->pcfg->pdb, NULL, &count, 0);

    if (ret) {
        return_error_tuple(pdrv, db_strerror(ret));
    } else {
        return_ok(pdrv);
    }

    return;

}

#if ((DB_VERSION_MAJOR > 4) || ((DB_VERSION_MAJOR >= 4) && (DB_VERSION_MINOR > 4)))

static void process_add_replication_node( bdb_drv_t* pdrv, ErlIOVec *ev) {
    ErlDrvBinary* data = ev->binv[1];

    char *bytes = data->orig_bytes;

    char *if_len_bytes = bytes + 1;
    char *if_bytes = if_len_bytes + 4;
    uint32_t if_len = (uint32_t) ntohl(* ((uint32_t*) if_len_bytes) );

    char *if_port_bytes = if_bytes + if_len;
    uint32_t if_port = (uint32_t) ntohl(* ((uint32_t*) if_port_bytes) );

    int ret ;

    char ip_addr[256];

    memcpy(ip_addr, if_bytes, if_len);

    ip_addr[if_len] = 0;

    if (pdrv->pcfg) {

        if ((ret = pdrv->pcfg->penv->repmgr_add_remote_site(pdrv->pcfg->penv, ip_addr, if_port, NULL, 0)) == 0) {
            return_ok(pdrv);
        } else {
            return_error_tuple(pdrv, db_strerror(ret));
        }

    } else {

        return_error_tuple(pdrv, "Database not opened!");
    }

}
#endif


static void process_bulk_get( bdb_drv_t* pdrv, ErlIOVec *ev) {
    ErlDrvBinary* data = ev->binv[1];

    char *bytes = data->orig_bytes;

    char *offset_bytes = bytes + 1;
    unsigned int offset = (unsigned int) ntohl(* ((uint32_t*) offset_bytes) );

    char *count_bytes = bytes + 1 + 4;
    unsigned int count = (unsigned int) ntohl(* ((uint32_t*) count_bytes) );

    if (pdrv->pcfg) {
        if (pdrv->pcfg->db_type == DB_HASH) {
            bulk_get_hash (offset, count, pdrv);
        } else {
            bulk_get_btree (offset, count, pdrv);
        }
    } else {
        return_error_tuple(pdrv, "Database not opened!");
    }

}


static void bulk_get_btree (u_int32_t offset, u_int32_t count, bdb_drv_t *pdrv) {

    int ret;
    size_t retklen, retdlen;
    void *retkey, *retdata;
    void* p;

    DB*  pdb;
    DBC* pdbc = NULL;
    DBT  key, data;
   
    db_recno_t curr         = 1;
    db_recno_t limit        = 1;
    db_recno_t actual_count = 0;

    ErlDrvTermData *spec;
 
    int spec_items, idx;

    u_int32_t flags;

    curr  = offset;
    limit = offset + count;

    pdb = pdrv->pcfg->pdb;

    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));

    data.data  = pdrv->pcfg->buffer;
    data.ulen  = pdrv->pcfg->bulk_get_buffer_size_bytes;
    data.flags = DB_DBT_USERMEM;

    if ((ret = pdb->cursor(pdb, NULL, &pdbc, 0)) != 0) {
        return_error_tuple(pdrv, db_strerror(ret));
        return;
    }

    key.data = &curr;
    key.size = sizeof(curr);

    flags = DB_MULTIPLE_KEY | DB_SET_RECNO;

    if ((ret = pdbc->c_get(pdbc, &key, &data, flags)) != 0) {

        if (ret == DB_NOTFOUND) {
            return_ok_empty_list(pdrv);
        } else {
            return_error_tuple(pdrv, db_strerror(ret));
        }

    } else {

        //First count the number of recs...
        for (DB_MULTIPLE_INIT(p, &data); curr < limit; curr++) {


//DB_MULTIPLE_KEY_NEXT(void *pointer, DBT *data, void *retkey, size_t retklen, void *retdata, size_t retdlen);
            DB_MULTIPLE_KEY_NEXT(p, &data, retkey, retklen, retdata, retdlen);

            if (p == NULL) {
                break;
            } else {
                actual_count++;
            }

        }

        spec_items = (8 * actual_count) + 7;

        spec = malloc(sizeof(ErlDrvTermData) * spec_items);

        if (spec == NULL) {
            return_error_tuple(pdrv, "Could not allocate memory for operation!");
        } else {

            spec[0] = ERL_DRV_ATOM;
            spec[1] = driver_mk_atom("ok");

            //Now pipe the data...
            p    = NULL;
            curr = 0;

            for (DB_MULTIPLE_INIT(p, &data); curr < actual_count; curr++) {

                DB_MULTIPLE_KEY_NEXT(p, &data, retkey, retklen, retdata, retdlen);

                if (p == NULL) {
                    break;
                } else {

                    idx = 2 + (curr * 8);

                    spec[idx + 0] = ERL_DRV_STRING;
                    spec[idx + 1] = (ErlDrvTermData)retkey;
                    spec[idx + 2] = retklen;

                    spec[idx + 3] = ERL_DRV_STRING;
                    spec[idx + 4] = (ErlDrvTermData)retdata;
                    spec[idx + 5] = retdlen;

                    spec[idx + 6] = ERL_DRV_TUPLE;
                    spec[idx + 7] = 2;
 
                }

            }

            spec[spec_items - 5] = ERL_DRV_NIL;

            spec[spec_items - 4] = ERL_DRV_LIST;
            spec[spec_items - 3] = actual_count + 1;

            spec[spec_items - 2] = ERL_DRV_TUPLE;
            spec[spec_items - 1] = 2;

#if ((ERL_DRV_EXTENDED_MAJOR_VERSION == 1) || ((ERL_DRV_EXTENDED_MAJOR_VERSION == 2) && (ERL_DRV_EXTENDED_MINOR_VERSION == 0)))
            driver_output_term(pdrv->port, spec, spec_items);
#else
            ErlDrvTermData mkport = driver_mk_port(pdrv->port);
            erl_drv_output_term(mkport, spec, spec_items);
#endif

            free(spec);

        }

    }

    pdbc->c_close(pdbc);

    return;

 }

static void bulk_get_hash (u_int32_t offset, u_int32_t count, bdb_drv_t *pdrv) {

    int ret;
    size_t retklen, retdlen;
    void *retkey, *retdata;
    void* p;

    DB*  pdb;
    DBC* pdbc = NULL;
    DBT  key, data;
   
    db_recno_t curr         = 1;
    db_recno_t limit        = 1;
    db_recno_t actual_count = 0;

    ErlDrvTermData *spec;
 
    u_int32_t spec_items, idx;

    curr  = offset;
    limit = offset + count;

    pdb = pdrv->pcfg->pdb;

    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));

    data.data  = pdrv->pcfg->buffer;
    data.ulen  = pdrv->pcfg->bulk_get_buffer_size_bytes;
    data.flags = DB_DBT_USERMEM;

    if ((ret = pdb->cursor(pdb, NULL, &pdbc, 0)) != 0) {
        return_error_tuple(pdrv, db_strerror(ret));
        return;
    }

    //Fast forward to the correct index
    idx = 1;

    while ((ret = pdbc->c_get(pdbc, &key, &data, DB_NEXT)) == 0) {

        idx++;

        if (idx >= offset) { break; }
    }

    if ((ret != DB_NOTFOUND) && (ret != 0)) {
        return_error_tuple(pdrv, db_strerror(ret));

    } else if (ret == DB_NOTFOUND) {
        return_ok_empty_list(pdrv);

    } else {

            if ((ret = pdbc->c_get(pdbc, &key, &data, DB_MULTIPLE_KEY | DB_CURRENT)) != 0) {

                if (ret == DB_NOTFOUND) {
                return_ok_empty_list(pdrv);
                } else {
                    return_error_tuple(pdrv, db_strerror(ret));
                }

            } else {

                //First count the number of recs...
                for (DB_MULTIPLE_INIT(p, &data); curr < limit; curr++) {

                    DB_MULTIPLE_KEY_NEXT(p, &data, retkey, retklen, retdata, retdlen);

                    if (p == NULL) {
                        break;
                    } else {
                        actual_count++;
                    }
        
                }

                spec_items = (8 * actual_count) + 7;

                spec = malloc(sizeof(ErlDrvTermData) * spec_items);

                if (spec == NULL) {
                    return_error_tuple(pdrv, "Could not allocate memory for operation!");
                } else {

                    spec[0] = ERL_DRV_ATOM;
                    spec[1] = driver_mk_atom("ok");

                    //Now pipe the data...
                    p    = NULL;
                    curr = 0;

                    for (DB_MULTIPLE_INIT(p, &data); curr < actual_count; curr++) {

                        DB_MULTIPLE_KEY_NEXT(p, &data, retkey, retklen, retdata, retdlen);

                        if (p == NULL) {
                            break;
                        } else {

                            idx = 2 + (curr * 8);

                            spec[idx + 0] = ERL_DRV_STRING;
                            spec[idx + 1] = (ErlDrvTermData)retkey;
                            spec[idx + 2] = retklen;

                            spec[idx + 3] = ERL_DRV_STRING;
                            spec[idx + 4] = (ErlDrvTermData)retdata;
                            spec[idx + 5] = retdlen;

                            spec[idx + 6] = ERL_DRV_TUPLE;
                            spec[idx + 7] = 2;
     
                       }

                    }

                    spec[spec_items - 5] = ERL_DRV_NIL;

                    spec[spec_items - 4] = ERL_DRV_LIST;
                    spec[spec_items - 3] = actual_count + 1;

                    spec[spec_items - 2] = ERL_DRV_TUPLE;
                    spec[spec_items - 1] = 2;

#if ((ERL_DRV_EXTENDED_MAJOR_VERSION == 1) || ((ERL_DRV_EXTENDED_MAJOR_VERSION == 2) && (ERL_DRV_EXTENDED_MINOR_VERSION == 0)))
                    driver_output_term(pdrv->port, spec, spec_items);
#else
                    ErlDrvTermData mkport = driver_mk_port(pdrv->port);
                    erl_drv_output_term(mkport, spec, spec_items);
#endif

                    free(spec);

                }


        }

    }


    pdbc->c_close(pdbc);

    return;

 }


static void set (u_int32_t key_size, void* praw_key, u_int32_t data_size, void* praw_data, bdb_drv_t *pdrv) {

    DB* pdb = pdrv->pcfg->pdb;

    DBT key;
    DBT data;

    int ret, count;

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.data = praw_key,
    key.size = key_size,
    
    data.data = praw_data,
    data.size = data_size,

    count = 0;

    while (1) {

        ret = pdb->put(pdb, 0, &key, &data, 0);
    
        if        (ret == 0) {
            return_ok(pdrv);
            break;
        } else if (ret == DB_LOCK_DEADLOCK) {
            if (count < 5) {
                count = count + 1;
                usleep(1000);
            } else {
                return_error_tuple(pdrv, db_strerror(ret));
                break;
            }

        } else {
            return_error_tuple(pdrv, db_strerror(ret));
            break;
        }

    }
        
    return;

}

static void get (u_int32_t key_size, void* praw_key, bdb_drv_t *pdrv) {

    DB* pdb = pdrv->pcfg->pdb;

    DBT key;
    DBT data;

    int ret, count;

    memset(&key, 0, sizeof(DBT));
    memset(&data, 0, sizeof(DBT));

    key.data   = praw_key,
    key.size   = key_size,
    data.flags = DB_DBT_MALLOC;

    count = 0;

    while (1) {

        ret = pdb->get(pdb, 0, &key, &data, 0);

        if        (ret == 0) {

            ErlDrvBinary* pbin = driver_alloc_binary(data.size);

            memcpy(pbin->orig_bytes, data.data, data.size);

            ErlDrvTermData spec[] = {   
                                    ERL_DRV_ATOM, driver_mk_atom("ok"),

                                    ERL_DRV_BINARY, (ErlDrvSInt) pbin, (ErlDrvUInt) data.size, (ErlDrvUInt) 0,

//                                    ERL_DRV_STRING, (ErlDrvTermData) data.data, data.size,
                 
                                    ERL_DRV_TUPLE, 2};

#if ((ERL_DRV_EXTENDED_MAJOR_VERSION == 1) || ((ERL_DRV_EXTENDED_MAJOR_VERSION == 2) && (ERL_DRV_EXTENDED_MINOR_VERSION == 0)))
            driver_output_term(pdrv->port, spec, sizeof(spec) / sizeof(spec[0]));
#else
            ErlDrvTermData mkport = driver_mk_port(pdrv->port);
            erl_drv_output_term(mkport, spec, sizeof(spec) / sizeof(spec[0]));
#endif

            free(data.data);

            driver_free_binary(pbin);

            break;
        } else if (ret == DB_LOCK_DEADLOCK) {
            if (count < 5) {
                count = count + 1;
                usleep(1000);
            } else {
                return_error_tuple(pdrv, db_strerror(ret));
                break;
            }

        } else {

            if (ret != DB_NOTFOUND) {
                return_error_tuple(pdrv, db_strerror(ret));
            } else {
                return_error_tuple(pdrv, "not_found");
            }

            break;
        }

    }

    return;

}

static void del (u_int32_t key_size, void* praw_key, bdb_drv_t *pdrv) {

    DB* pdb = pdrv->pcfg->pdb;

    DBT key;

    int ret, count;

    memset(&key, 0, sizeof(DBT));

    key.data   = praw_key,
    key.size   = key_size,

    count = 0;

    while (1) {

        ret = pdb->del(pdb, NULL, &key, 0);

        if        ((ret == 0) || (ret == DB_NOTFOUND)) {
            return_ok(pdrv);
            break;
        } else if (ret == DB_LOCK_DEADLOCK) {
            if (count < 5) {
                count = count + 1;
                usleep(1000);
            } else {
                return_error_tuple(pdrv, db_strerror(ret));
                break;
            }

        } else {
            return_error_tuple(pdrv, db_strerror(ret));
            break;
        }

    }

    return;

}

#if ((DB_VERSION_MAJOR > 4) || ((DB_VERSION_MAJOR >= 4) && (DB_VERSION_MINOR > 4)))

static void event_callback(DB_ENV* penv, u_int32_t event, void* info) {

    drv_cfg* pcfg;

    pcfg = (drv_cfg*) penv->app_private;

    int ret;

    DB* pdb;

    switch (event) {
    case DB_EVENT_PANIC:
        //fprintf(stderr, "DB_EVENT_PANIC\n");
        break;
    case DB_EVENT_REP_CLIENT:
        //fprintf(stderr, "DB_EVENT_REP_CLIENT\n");
        pcfg->is_master = 0;
        break;

    case DB_EVENT_REP_ELECTED:
        //fprintf(stderr, "DB_EVENT_REP_ELECTED\n");
        break;

    case DB_EVENT_REP_MASTER:
        //fprintf(stderr, "DB_EVENT_REP_MASTER\n");
        pcfg->is_master = 1;
        goto do_open_db;
        break;
 
    case DB_EVENT_REP_NEWMASTER:

        //fprintf(stderr, "DB_EVENT_REP_NEWMASTER\n");
        if (pcfg->pdb == NULL) {
            goto do_open_db;
        }

        break;

    case DB_EVENT_REP_PERM_FAILED:
        //fprintf(stderr, "DB_EVENT_REP_PERM_FAILED\n");
        break;

    case DB_EVENT_REP_STARTUPDONE:
        goto do_open_db;

        break;

    case DB_EVENT_WRITE_FAILED:
        //fprintf(stderr, "DB_EVENT_REP_PERM_FAILED\n");
        break;

    default:
        break;
    }

    return;

do_open_db:

        if ((ret = db_create(&pdb, penv, 0)) != 0) {
            return;
        }

        if (pcfg->db_type == DB_BTREE) {
            if ((ret = pdb->set_flags(pdb, DB_RECNUM)) != 0) {
                return;
            }
        }

        if ((ret = pdb->set_pagesize(pdb, pcfg->page_size_bytes)) != 0) {
            return;
        }

        pdb->set_errpfx(pdb, pcfg->db_name);

        if (pcfg->is_master) {
            pcfg->db_open_flags = DB_CREATE | DB_THREAD | DB_AUTO_COMMIT;
        } else {
            pcfg->db_open_flags = DB_THREAD | DB_AUTO_COMMIT;
        }

        if ((ret = pdb->open(pdb, NULL, pcfg->db_name, pcfg->db_name, pcfg->db_type, pcfg->db_open_flags, 0)) != 0) {
            return;
        }

        pcfg->pdb  = pdb;


}

#endif

//EOF
