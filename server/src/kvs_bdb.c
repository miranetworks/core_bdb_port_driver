//$Id$
#include "kvs_bdb.h"
pthread_mutex_t access_mutex = PTHREAD_MUTEX_INITIALIZER;

void* server_thread     (void *args);
void* worker_thread     (void *args);
void* sync_thread       (void *args);
void* deadlock_thread   (void *args);

void shutdown_handler (int signal);

int     transactions_enabled = 0;

int     logging_enabled = 0;
int     log_to_file     = 0;
char*   log_filename    = "";
FILE*   log_fd          = 0;
#define LOG_DEBUG 0
#define LOG_INFO  1
#define LOG_ERROR 2
int     log_level       = LOG_INFO;
char*   log_tags[LOG_ERROR + 1] = {"DEBUG", "INFO", "ERROR"};
FILE*   open_log(int log_to_file, const char* log_filename);
void    kvslog(int level, const char* fmt, ...);

int do_shutdown = 0;
pthread_t server_tid, sync_tid, deadlock_tid;

void usage () {

    printf("\n\nKey-Value-Store using Berkeley DB\n" \
           "svn rev: $Id$\n\n" \
           "Usage:\n\n   kvs_bdb  [-h] [-d DIR] [-n DBNAME] [-i IFACE] [-p PORT] [-s SYNC_TIME] \n" \
           "            [-b PAGE_BYTES] [-c CACHE_BYTES] [-g GET_BUFFER] [-l]\n" \
           "            [-T] [-f LOG_FILENAME] [-v LOG_LEVEL]\n\n");

    printf("  -h              print this help and exit\n");
    printf("  -d DIR          set the directory where data file(s) are stored\n");
    printf("  -n DBNAME       set the database name\n");
    printf("  -i IFACE        set the tcp/ip inteface to bind to\n");
    printf("  -p PORT         set the tcp/ip port to listen on\n");
    printf("  -s SYNC_TIME    set the sync (flushing to disk) interval in milliseconds (0 to disable)\n");
    printf("  -b PAGE_SIZE    set the page size in bytes\n");
    printf("  -c CACHE_SIZE   set the cache size in megabytes\n");
    printf("  -g GET_BUFFER   set the bulk get buffer size in megabytes\n");
    printf("  -t TCP_TIMEOUT  set the client tcp tx/rx timeout in seconds\n");
    printf("  -l              enable logging\n");
    printf("  -f LOG_FILENAME log to a file\n");
    printf("  -T              enable transactions\n");
    printf("  -v LOG_LEVEL    0 - DEBUG (very chatty), 1 - INFO (less chatty), 2 - ERROR (only errors)\n\n\n");

}

int main(int argc, char *argv[]) {
    app_cfg cfg;
    int ret, opt;

    cfg.data_dir             = ".";
    cfg.db_name              = "data";
    cfg.tcp_interface        = "127.0.0.1";
    cfg.tcp_port             = 22122;
    cfg.sync_interval_ms     = 5000;
    cfg.deadlock_interval_ms = 100;
    cfg.page_size_bytes      = 4096;
    cfg.cache_size_bytes     = 16 * 1024 * 1024;
    cfg.bulk_get_buffer_size_bytes = 1 * 1024 * 1024; //Default to 1 MB for now
    cfg.tcp_timeout_enabled        = 0;
    cfg.tcp_timeout_secs           = 60;

    while((opt = getopt(argc, argv, "hd:n:i:p:s:b:c:lg:t:f:v:T")) != -1) {

        switch(opt) {
        case 'h':
            usage();
            exit(0);
    
        case 'd':
            cfg.data_dir = optarg;
            break;

        case 'n':
            cfg.db_name = optarg;           
            break;

        case 'i':
            cfg.tcp_interface = optarg;
            break;

        case 'p':
            cfg.tcp_port = atoi(optarg);
            break;

        case 's':
            cfg.sync_interval_ms = atoi(optarg);
            break;

        case 'b':
            cfg.page_size_bytes = atoi(optarg);
            break;

        case 'c':
            cfg.cache_size_bytes = atoi(optarg) * 1024 * 1024;
            break;

        case 'g':
            cfg.bulk_get_buffer_size_bytes = atoi(optarg) * 1024 * 1024;
            break;

        case 'l':
            logging_enabled = 1;
            break;

        case 'T':
            transactions_enabled = 1;
            break;

        case 'v':
            log_level = atoi(optarg);
            break;

        case 'f':
            log_to_file  = 1;
            log_filename = optarg;
            break;


        case 't':

            cfg.tcp_timeout_enabled = 1;
            cfg.tcp_timeout_secs    = atoi(optarg);

            break;


        default:
            usage();
            exit(-1);
        }

    }

    if ((log_fd = open_log(log_to_file, log_filename)) == NULL) {
        exit(-1);
    }

    kvslog (LOG_INFO, "Starting...");

    ret = make_server_socket(&cfg);
    if (ret != 0) {
        kvslog (LOG_ERROR, "Exit - make_server_socket");
        exit(-1);
    }

    ret = open_db(&cfg);
    if (ret != 0) {
        kvslog (LOG_ERROR, "Exit - open_db");
        exit (-1);
    }

    pthread_create (&deadlock_tid, NULL, deadlock_thread, (void*)&cfg);

    pthread_create (&sync_tid, NULL, sync_thread, (void*)&cfg);

    pthread_create (&server_tid, NULL, server_thread, (void*)&cfg);
    
    signal(SIGINT, shutdown_handler);
    signal(SIGHUP, shutdown_handler);
    signal(SIGTERM, shutdown_handler);

    pthread_join(server_tid, NULL);

    kvslog(LOG_INFO, "Waiting for Deadlock thread...");
    pthread_join(deadlock_tid, NULL);

    kvslog(LOG_INFO, "Waiting for Sync thread...");
    pthread_join(sync_tid, NULL);

    kvslog(LOG_INFO, "Final sync...");
    cfg.pdb->close(cfg.pdb, 0);
    kvslog(LOG_INFO, "Done.");

    kvslog(LOG_INFO, "Shutdown complete.");

    exit(0);

}

void shutdown_handler(int signal) {

    switch (signal) {
    case SIGINT:
    case SIGTERM:
    case SIGHUP:

        do_shutdown = 1;

        kvslog(LOG_INFO, "Got SIGINT/SIGHUP/SIGTERM signal, shutting down...");
        pthread_cancel(server_tid);

        break;
    default:
        break;
    }

}

FILE* open_log(int log_to_file, const char* log_filename) {

FILE* fd;

    if (log_to_file) {
        fd = fopen(log_filename, "a");

        if (fd == NULL) {
            printf("Could not open %s for writing!\nPlease make sure the path exists and\nthat you have sufficient access rights.\n", log_filename);
        }

        return fd;

    } else {
        return stdout;
    }

}

int open_db(app_cfg* pcfg) {
    DB* pdb;
    DB_ENV* penv;
    u_int32_t open_flags;
    int ret;

    char filename[1024];

    ret = db_env_create(&penv, 0);
    if (ret != 0) {
        kvslog(LOG_ERROR, "Error creating environment handle: %s", db_strerror(ret));
        return -1;
    } 


    if (transactions_enabled) {
        open_flags = DB_CREATE | DB_INIT_MPOOL | DB_INIT_LOCK | DB_THREAD | DB_INIT_TXN | DB_INIT_LOG;
    } else {
        open_flags = DB_CREATE | DB_INIT_MPOOL | DB_INIT_LOCK | DB_THREAD;
    }

    ret = penv->set_cachesize(penv, 0, pcfg->cache_size_bytes, 1);
    if (ret != 0) {
        kvslog(LOG_ERROR, "%s", db_strerror(ret));
        return -1;
    }

    ret = penv->open(penv, pcfg->data_dir, open_flags, 0);
    if (ret != 0) {
        kvslog(LOG_ERROR, "%s", db_strerror(ret));
        return -1;
    }

    ret = db_create(&pdb, penv, 0);
    if (ret != 0) {
        kvslog(LOG_ERROR, "%s", db_strerror(ret));
        return -1;
    }

    ret = pdb->set_flags(pdb, DB_RECNUM);
    if (ret != 0) {
        kvslog(LOG_ERROR, "%s", db_strerror(ret));
        return -1;
    }

    ret = pdb->set_pagesize(pdb, pcfg->page_size_bytes);
    if (ret != 0) {
        kvslog(LOG_ERROR, "%s", db_strerror(ret));
        return -1;
    }

    pdb->set_errfile(pdb, stderr);
    pdb->set_errpfx(pdb, pcfg->db_name);

    open_flags = DB_CREATE | DB_THREAD;    /* Allow database creation */

    sprintf(filename, "%s.db", pcfg->db_name); 

    /* Now open the database */
    ret = pdb->open(pdb,        /* Pointer to the database */
            NULL,       /* Txn pointer */
            filename,  /* File name */
            pcfg->db_name,       /* Logical db name */
            DB_BTREE,   /* Database type (using hash) */
            open_flags, /* Open flags */
            0);         /* File mode. Using defaults */

    if (ret != 0) {
        pdb->err(pdb, ret, "Database '%s' open failed.", filename);
        return -1;
    }

    pcfg->pdb = pdb;

    return 0;

}

int close_db(DB* pdb) {

    int ret;

    ret = pdb->close(pdb, 0);

    if (ret != 0) {
        kvslog(LOG_ERROR, "%s", db_strerror(ret));
    }

    return (0);

}


int set (DB* pdb, u_int32_t key_size, void* praw_key, u_int32_t data_size, void* praw_data) {

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
            ret = 0;
            break;
        } else if (ret == DB_LOCK_DEADLOCK) {
            if (count < 5) {
                count = count + 1;
                usleep(1000);
            } else {
                kvslog(LOG_ERROR, "PUT: %s", db_strerror(ret));
                ret = -1;
                break;
            }

        } else {
            kvslog(LOG_ERROR, "PUT: %s", db_strerror(ret));
            ret = -1;
            break;
        }

    }
        
    return ret;

}

int get (DB* pdb, u_int32_t key_size, void* praw_key, void** ppdata, int* pdata_len) {

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

            *ppdata    = (void*) data.data;
            *pdata_len = data.size;

            ret = 0;
            break;
        } else if (ret == DB_LOCK_DEADLOCK) {
            if (count < 5) {
                count = count + 1;
                usleep(1000);
            } else {
                kvslog(LOG_ERROR, "GET: %s", db_strerror(ret));
                ret = -1;
                break;
            }

        } else {

            if (ret != DB_NOTFOUND) {
                kvslog(LOG_ERROR, "GET: %s", db_strerror(ret));
            }
            ret = -1;
            break;
        }

    }

    return ret;

}

int del (DB* pdb, u_int32_t key_size, void* praw_key) {

    DBT key;

    int ret, count;

    memset(&key, 0, sizeof(DBT));

    key.data   = praw_key,
    key.size   = key_size,

    count = 0;

    while (1) {

        ret = pdb->del(pdb, NULL, &key, 0);

        if        ((ret == 0) || (ret == DB_NOTFOUND)) {
            ret = 0;
            break;
        } else if (ret == DB_LOCK_DEADLOCK) {
            if (count < 5) {
                count = count + 1;
                usleep(1000);
            } else {
                kvslog(LOG_ERROR, "DEL: %s", db_strerror(ret));
                ret = -1;
                break;
            }

        } else {
            kvslog(LOG_ERROR, "DEL: %s", db_strerror(ret));
            ret = -1;
            break;
        }

    }

    return ret;

}

int make_server_socket(app_cfg* pcfg) {

    int ret, server_fd, sockoption;
    struct sockaddr_in server_addr;
 
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        kvslog (LOG_ERROR, "Could not create server socket %d", server_fd);
        return -1;
    }

    memset(&server_addr, 0, sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_port   = htons(pcfg->tcp_port);

    ret = inet_pton(AF_INET, pcfg->tcp_interface, &server_addr.sin_addr);
    if (ret != 1) {
        kvslog (LOG_ERROR, "Invalid interface %s",  pcfg->tcp_interface);
        return -1;
    } 

    sockoption = 1;
    if (setsockopt (server_fd, SOL_SOCKET, SO_REUSEADDR, (char *) &sockoption, sizeof(sockoption)) != 0) {
        kvslog(LOG_ERROR, "setsockopt [SO_REUSEADDR] failed: %s", strerror(errno));
        return -1;
    }

    ret = bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr));
    if (ret < 0) {
        kvslog(LOG_ERROR, "Could not bind to %s:%d", pcfg->tcp_interface, pcfg->tcp_port);
        return -1;
    }

    ret = listen(server_fd, 1);
    if (ret != 0) {
        kvslog(LOG_ERROR, "Could not listen on socket %d", server_fd);
        return -1;
    }

    pcfg->server_fd = server_fd;

    return 0;        

}

void* server_thread (void *args) {
    app_cfg* pcfg;

    worker_cfg* pwcfg;

    int ret, client_fd, clilen, sockoption;
    struct sockaddr_in client_addr;
    pthread_t client_tid;

    pcfg = (app_cfg*) args;

    while (!do_shutdown) {

        client_fd = accept(pcfg->server_fd, (struct sockaddr *) &client_addr, &clilen);

        if (client_fd < 0) {
            kvslog(LOG_ERROR, "Could not accept client connection %d", client_fd);
            usleep(100000);
        } else {

            sockoption = 1;
            setsockopt (client_fd, IPPROTO_TCP, TCP_NODELAY, (char *) &sockoption, sizeof(sockoption));

            if (pcfg->tcp_timeout_enabled) {
                struct timeval stimeout;

                stimeout.tv_sec  = pcfg->tcp_timeout_secs;
                stimeout.tv_usec = 0;

                if (setsockopt (client_fd, SOL_SOCKET, SO_RCVTIMEO, (char*) &stimeout, sizeof(stimeout)) != 0) {
                    kvslog(LOG_DEBUG, "setsockopt [SO_RCVTIMEO] failed: %s", strerror(errno));
                }

                if (setsockopt (client_fd, SOL_SOCKET, SO_SNDTIMEO, (char*) &stimeout, sizeof(stimeout)) != 0) {
                    kvslog(LOG_ERROR, "setsockopt [SO_SNDTIMEO] failed: %s", strerror(errno));
                }

            }

            pwcfg = (worker_cfg*) malloc(sizeof(worker_cfg));       

            if (pwcfg != NULL) {
                memset(pwcfg, 0, sizeof(worker_cfg));

                pwcfg->pcfg = pcfg;
                pwcfg->fd   = client_fd;

                ret = pthread_create (&client_tid, NULL, worker_thread, (void*)pwcfg);

                if (ret != 0) {
                    kvslog(LOG_ERROR, "Could not spawn worker thread %d", ret);
                    close(client_fd);
                    memset(pwcfg, 0, sizeof(worker_cfg));
                    free(pwcfg);   
                    usleep(500000);
                }

            } else {
                kvslog(LOG_ERROR, "Could not allocate memory for worker!");  
                usleep(500000);
                close(client_fd);
            }
        }

    }

    close(pcfg->server_fd);

}

void* sync_thread (void *args) {
    app_cfg* pcfg;

    int steps, count, ret;

    pcfg = (app_cfg*) args;

    kvslog(LOG_INFO, "Sync thread started...");

    steps = (pcfg->sync_interval_ms / 100) + 1;
    count = 0;

    if (pcfg->sync_interval_ms <= 0) {
        kvslog(LOG_INFO, "Syncing disabled...");  
    }

    while (!do_shutdown) {

        if (pcfg->sync_interval_ms > 0) {

            if (count > steps) {

                kvslog(LOG_DEBUG, "Syncing started...");  

                ret = pcfg->pdb->sync(pcfg->pdb, 0);

                if (ret) {
                    kvslog(LOG_ERROR, "Syncing failed: %s", db_strerror(ret));
                } else {
                    kvslog(LOG_DEBUG, "Syncing done...");
                }

                count = 0;
            }        

            count = count + 1;

        }
        
        usleep(100000);
    }

    kvslog(LOG_INFO, "Sync thread finished...");
}

void* deadlock_thread (void *args) {

    app_cfg* pcfg;
    DB_ENV* penv;

    int num_locks = 0;
    int steps, count;

    pcfg = (app_cfg*) args;

    kvslog(LOG_INFO, "Deadlock thread started...");

    steps = (pcfg->deadlock_interval_ms / 100) + 1;
    count = 0;

    penv = pcfg->pdb->get_env(pcfg->pdb);
    while (!do_shutdown) {

        if (count > steps) {

            while (!do_shutdown) {
                num_locks = 0;

                penv->lock_detect(penv, 0, DB_LOCK_DEFAULT, &num_locks);

                if (num_locks == 0) {
                    break;
                } else {
                    kvslog(LOG_INFO, "Resolved %d deadlocks", num_locks);
                }
            }

            count = 0;

        }

        count = count + 1;

        usleep(100000);
    }

    kvslog(LOG_INFO, "Deadlock thread finished...");
}

int read_key(worker_cfg* pwcfg) {

    unsigned short key_len;

    int ret;

    pwcfg->key_len = 0;

    if (read(pwcfg->fd, &key_len, 2) == 2) {

        key_len = (unsigned short) ntohs(key_len);

        if (key_len > 0) {

            pwcfg->key_len = (int) key_len;

            memset(pwcfg->key_buffer, 0, sizeof(pwcfg->key_buffer));

            ret = read(pwcfg->fd, pwcfg->key_buffer, pwcfg->key_len);

            if (ret == pwcfg->key_len) {

                ret = 0;

            } else {
                kvslog(LOG_DEBUG, "Could not read key [%d]", ret);
                ret = -1;
            }  

        } else {
            kvslog(LOG_DEBUG, "Invalid key_len");
            ret = -1;
        }


    } else {
        kvslog(LOG_DEBUG, "Could not read key_len");
        ret = -1;
    }

    return ret;

}

int read_bulk_get_range(worker_cfg* pwcfg) {

    int ret;

    char buffer[8];

    pwcfg->bulk_get_offset = 1;
    pwcfg->bulk_get_count  = 1;

    if (read(pwcfg->fd, buffer, 8) == 8) {

        memcpy(&pwcfg->bulk_get_offset, &buffer[0], 4);
        memcpy(&pwcfg->bulk_get_count,  &buffer[4], 4);

        pwcfg->bulk_get_offset = ntohl(pwcfg->bulk_get_offset);
        pwcfg->bulk_get_count  = ntohl(pwcfg->bulk_get_count);

        if (pwcfg->bulk_get_offset < 1) {
            kvslog(LOG_DEBUG, "Invalid bulk_get_offset %d", pwcfg->bulk_get_offset);
            ret = -1;
        } else if (pwcfg->bulk_get_count < 1) {
            kvslog(LOG_DEBUG, "Invalid bulk_get_count %d", pwcfg->bulk_get_count);
            ret = -1;
        } else {
            ret = 0;
        }



    } else {
        kvslog(LOG_DEBUG, "Could not read bulk_get_offset, bulk_get_count");
        ret = -1;
    }

    return ret;

}

void free_data_buffer(worker_cfg* pwcfg) {

    if (pwcfg->data_buffer != NULL) {
        free(pwcfg->data_buffer);
        pwcfg->data_buffer = NULL;
        pwcfg->data_len    = 0;
    }

}

int read_data(worker_cfg* pwcfg) {

    unsigned short data_len;

    int ret;

    pwcfg->data_len = 0;

    if (read(pwcfg->fd, &data_len, 2) == 2) {

        data_len = (unsigned short) ntohs(data_len);

        if (data_len > 0) {

            pwcfg->data_buffer = malloc(data_len + 1);

            if (pwcfg->data_buffer != NULL) {

                pwcfg->data_len = (int) data_len;

                ((char*)pwcfg->data_buffer)[pwcfg->data_len] = '\0';

                ret = read(pwcfg->fd, pwcfg->data_buffer, pwcfg->data_len);

                if (ret == pwcfg->data_len) {

                    ret = 0;

               } else {

                    free_data_buffer(pwcfg);

                    kvslog(LOG_DEBUG, "Could not read key [%d]", ret);
                    ret = -1;
                }  

            } else {
                kvslog(LOG_DEBUG, "Could not allocate memory for data!");
                ret = -1;
            }


        } else {
            kvslog(LOG_DEBUG, "Invalid data_len");
            ret = -1;
        }


    } else {
        kvslog(LOG_DEBUG, "Could not read key_len");
        ret = -1;
    }

    return ret;

}

int reply_ack(worker_cfg* pwcfg) {

    int ret;

    if ((ret = write(pwcfg->fd, "A", 1)) == 1) {
        ret = 0;
    } else {
        kvslog(LOG_DEBUG, "Write error %d", ret);
        ret = -1;

    }

    return ret;
}

int reply_ack_with_data(worker_cfg* pwcfg) {

    unsigned short data_len;
    int ret;

    char len_buff[2];    

    if ((ret = write(pwcfg->fd, "A", 1)) == 1) {

        data_len = htons((unsigned short)pwcfg->data_len);

        memcpy(len_buff, &data_len, 2);

        if ((ret = write(pwcfg->fd, len_buff, 2)) == 2) {

            if ((ret = write(pwcfg->fd, pwcfg->data_buffer, pwcfg->data_len)) == pwcfg->data_len) {

                ret = 0;

            } else {
                kvslog(LOG_DEBUG, "Write error %d", ret);
                ret = -1;

            }

        } else {
            kvslog(LOG_DEBUG, "Write error %d", ret);
            ret = -1;

        }


    } else {
        kvslog(LOG_DEBUG, "Write error %d", ret);
        ret = -1;

    }

    return ret;
}

int reply_hdr4_with_data(worker_cfg* pwcfg, int buffer_len, char* buffer) {
    unsigned int data_len;
    int ret;

    char len_buff[4];    

    data_len = htonl((unsigned short)buffer_len);

    memcpy(len_buff, &data_len, 4);

    if ((ret = write(pwcfg->fd, len_buff, 4)) == 4) {

        if ((ret = write(pwcfg->fd, buffer, buffer_len)) == buffer_len) {

            ret = 0;

        } else {
            kvslog(LOG_DEBUG, "Write error %d", ret);
            ret = -1;

        }

    } else {
        kvslog(LOG_DEBUG, "Write error %d", ret);
        ret = -1;

    }

    return ret;

}

int reply_raw_data(worker_cfg* pwcfg, int buffer_len, char* buffer) {

    int ret;

    if ((ret = write(pwcfg->fd, buffer, buffer_len)) == buffer_len) {

        ret = 0;

    } else {
        kvslog(LOG_DEBUG, "Write error %d", ret);
        ret = -1;
    }

    return ret;
}


int reply_nack(worker_cfg* pwcfg) {

    int ret;

    if ((ret = write(pwcfg->fd, "N", 1)) == 1) {
        ret = 0;

    } else {
        kvslog(LOG_DEBUG, "Write error %d", ret);
        ret = -1;

    }

    return ret;

}

int process_set( worker_cfg* pwcfg) {

    int ret;

    if (read_data(pwcfg) == 0) {

        //pthread_mutex_lock( &access_mutex );
        ret = set (pwcfg->pcfg->pdb, (u_int32_t) pwcfg->key_len, pwcfg->key_buffer, (u_int32_t) pwcfg->data_len, pwcfg->data_buffer);
        //pthread_mutex_unlock( &access_mutex );

        free_data_buffer(pwcfg);
        
        if (ret == 0) {

            ret = reply_ack(pwcfg);
           
        } else {

            ret = reply_nack(pwcfg);

        }

    } else {
        ret = -1;
    }


    return ret;
}

int process_get( worker_cfg* pwcfg) {
    int ret;

    //pthread_mutex_lock( &access_mutex );
    ret = get (pwcfg->pcfg->pdb, (u_int32_t) pwcfg->key_len, pwcfg->key_buffer, &pwcfg->data_buffer, &pwcfg->data_len);
    //pthread_mutex_unlock( &access_mutex );

    if (ret == 0) {

        ret = reply_ack_with_data(pwcfg);
    
        free_data_buffer(pwcfg);
       
    } else {

        ret = reply_nack(pwcfg);

    }

    return ret;
}

int process_remove( worker_cfg* pwcfg) {

    int ret;

    pthread_mutex_lock( &access_mutex );
    ret = del (pwcfg->pcfg->pdb, (u_int32_t) pwcfg->key_len, pwcfg->key_buffer);
    pthread_mutex_unlock( &access_mutex );

    if (ret == 0) {
        ret = reply_ack(pwcfg);
    } else {

        ret = reply_nack(pwcfg);
    }

    return ret;

}

int process_count( worker_cfg* pwcfg) {

    u_int32_t countp;

    DB_BTREE_STAT* stats;

    char str_size [64];

    int ret, count;

    count = 0;

    while (1) {

        //pthread_mutex_lock( &access_mutex );
        ret = pwcfg->pcfg->pdb->stat(pwcfg->pcfg->pdb, NULL, &stats, DB_FAST_STAT);
        //pthread_mutex_unlock( &access_mutex );

        if (ret != 0) {

            kvslog(LOG_ERROR, "%s", db_strerror(ret));

            if (count > 2) {
                ret = reply_nack(pwcfg);
                break;
            } else {
                count = count + 1;
                usleep(1000);
            }

       } else {

            sprintf(str_size, "%u", stats->bt_nkeys);

            pwcfg->data_buffer = (void*) str_size;
            pwcfg->data_len    = strlen(str_size);

            free(stats);

            ret = reply_ack_with_data(pwcfg);

            pwcfg->data_buffer = NULL;
            pwcfg->data_len    = 0;

            break;
        }

    }

    return ret;
}

int process_flush( worker_cfg* pwcfg) {
    int ret;


    kvslog(LOG_DEBUG, "Syncing started...");

    ret = pwcfg->pcfg->pdb->sync(pwcfg->pcfg->pdb, 0);

    if (ret) {
        kvslog(LOG_ERROR, "Syncing failed: %s", db_strerror(ret));
        ret = reply_nack(pwcfg);
    } else {
        kvslog(LOG_DEBUG, "Syncing done...");
        ret = reply_ack(pwcfg);
    }

    return ret;
}


int process_keepalive( worker_cfg* pwcfg) {
    int ret;

    ret = reply_ack(pwcfg);

    return ret;
}

int process_bulk_get( worker_cfg* pwcfg) {

    int ret;
    size_t retklen, retdlen;
    char *retkey, *retdata;
    void* p;

    DB*  pdb;
    DBC* pdbc = NULL;
    DBT  key, data;
    
    unsigned int data_len;
    char  reply_buff[1024];

    char* buffer            = NULL;
    db_recno_t curr         = 1; 
    db_recno_t limit        = 1;
    db_recno_t actual_count = 0;

    buffer = (char*) malloc(pwcfg->pcfg->bulk_get_buffer_size_bytes);
    if (buffer == NULL) {
        ret = reply_nack(pwcfg);
        return ret;
    }    

    curr  = pwcfg->bulk_get_offset;
    limit = pwcfg->bulk_get_offset + pwcfg->bulk_get_count;

    pdb = pwcfg->pcfg->pdb;

    memset(&key, 0, sizeof(key)); 
    memset(&data, 0, sizeof(data));

    data.data  = buffer;
    data.ulen  = pwcfg->pcfg->bulk_get_buffer_size_bytes;
    data.flags = DB_DBT_USERMEM;

    if ((ret = pdb->cursor(pdb, NULL, &pdbc, 0)) != 0) { 
        kvslog(LOG_ERROR, "%s", db_strerror(ret));
        free(buffer);
        return reply_nack(pwcfg);
    }

    key.data = &curr;
    key.size = sizeof(curr);

    if ((ret = pdbc->c_get(pdbc, &key, &data, DB_MULTIPLE_KEY | DB_SET_RECNO)) != 0) {

        if (ret == DB_NOTFOUND) {

            reply_buff[0] = 'A';
            reply_buff[1] = 0;
            reply_buff[2] = 0;
            reply_buff[3] = 0;
            reply_buff[4] = 0;

            ret = reply_raw_data(pwcfg, 5, reply_buff);

        } else {
            kvslog(LOG_ERROR, "%s", db_strerror(ret));
            ret = reply_nack(pwcfg);
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

        reply_buff[0] = 'A';

        data_len = htonl((unsigned int)actual_count);

        memcpy(&reply_buff[1], &data_len, sizeof(data_len)); 

        if ((ret = reply_raw_data(pwcfg, 5, reply_buff)) == 0) {

            //Now pipe the data...
            p    = NULL;
            curr = 0;

            for (DB_MULTIPLE_INIT(p, &data); curr < actual_count; curr++) { 

                DB_MULTIPLE_KEY_NEXT(p, &data, retkey, retklen, retdata, retdlen); 

                if (p == NULL) {
                    break;
                } else {

                    if ((ret = reply_hdr4_with_data(pwcfg, retklen, retkey)) == 0) {

                        if ((ret = reply_hdr4_with_data(pwcfg, retdlen, retdata)) != 0) {
                            break;
                        }

                    } else {
                        break;
                    }

                }

            }

        }

    }

    if (buffer != NULL) {
        free(buffer);
    }

    int c_ret;

    if ((c_ret = pdbc->c_close(pdbc)) != 0) { 
        kvslog(LOG_ERROR, "%s", db_strerror(c_ret));

        ret = -1;
    }

    return ret;

}

int process_further(char cmd, worker_cfg* pwcfg) {

    switch ((char) cmd) {

    case 'S':
        return process_set(pwcfg);
    case 'G':
        return process_get(pwcfg);
    case 'R':
        return process_remove(pwcfg);
    case 'C':
        return process_count(pwcfg);
    case 'K':
        return process_keepalive(pwcfg);
    case 'B':
        return process_bulk_get(pwcfg);
    case 'F':
        return process_flush(pwcfg);

    default:
        kvslog(LOG_DEBUG, "Unknown cmd 0x%2.2x", cmd);
        return -1;

    }

}

int process_command(char cmd,  worker_cfg* pwcfg) {

    switch ((char) cmd) {

    case 'S':
    case 'G':
    case 'R':

        if (read_key(pwcfg) == 0 ) {

            return process_further(cmd, pwcfg);

        } else {
            return -1;
        }

        break;

    case 'B':

        if (read_bulk_get_range(pwcfg) == 0 ) {
            return process_further(cmd, pwcfg);

        } else {
            return -1;
        }

        break;

    case 'F':
    case 'C':
    case 'K':
            return process_further(cmd, pwcfg);
        break;

    default:
        kvslog(LOG_DEBUG, "Unknown cmd 0x%2.2x", cmd);
        return -1;

    }

}


void* worker_thread (void *args) {

    worker_cfg* pwcfg;

    pwcfg = (worker_cfg*) args;
    char cmd;

    kvslog(LOG_DEBUG, "Worker thread started.");

    while (!do_shutdown) {

        if (read(pwcfg->fd, &cmd, 1) == 1) {

            if (process_command(cmd, pwcfg) == 0) {

            } else {
                break;
            }       

        } else {
            break;
        }       

    }

    close(pwcfg->fd);

    memset(pwcfg, 0, sizeof(worker_cfg));

    free(pwcfg);

    kvslog(LOG_DEBUG, "Worker thread done.");

}



void kvslog(int level, const char* fmt, ...) {
va_list ap;
FILE*   fd;

char buffer[256];
time_t curtime;
struct tm *loctime;

    if (logging_enabled && (level >= log_level) && 
            ((level >= LOG_DEBUG) && (level <= LOG_ERROR))) {

        curtime = time (NULL);
        loctime = localtime (&curtime);
        strftime (buffer, 256, "%Y-%m-%d %H:%M:%S %z", loctime);


        if (log_to_file) {
            fd = log_fd;
        } else {
            fd = stdout;
        }

        va_start (ap, fmt);

        flockfile (fd);

        fprintf(fd, "[%s] %s ", buffer, log_tags[level]);
        vfprintf(fd, fmt, ap);
        fprintf(fd, "\n");

        fflush_unlocked(fd);

        funlockfile (fd);

        va_end (ap);
    }

}

//EOF
