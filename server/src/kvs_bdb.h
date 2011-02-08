//$Id$

#include <db.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <getopt.h>
#include <arpa/inet.h>


#ifndef __KVS_BDB_H__
#define __KVS_BDB_H__

typedef struct _app_cfg app_cfg;

struct _app_cfg {
    DB*             pdb;
    
    const char* data_dir;
    const char* db_name;
    const char* tcp_interface;

    int   tcp_port;
    int   sync_interval_ms;
    int   deadlock_interval_ms;
    int   page_size_bytes;
    int   cache_size_bytes;

    int   server_fd;

    int   bulk_get_buffer_size_bytes;

    int   tcp_timeout_enabled;
    int   tcp_timeout_secs;

};

typedef struct _worker_cfg worker_cfg;

struct _worker_cfg {

    app_cfg*     pcfg;
    int           fd;

    unsigned char key_buffer[65536];
    int           key_len;

    void*         data_buffer;
    int           data_len;

    unsigned int  bulk_get_offset;
    unsigned int  bulk_get_count;

};


#endif

//EOF
