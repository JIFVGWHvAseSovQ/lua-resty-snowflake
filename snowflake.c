/**
 * original version(scala):
 * https://github.com/twitter-archive/snowflake/blob/snowflake-2010/src/main/scala/com/twitter/service/snowflake/IdWorker.scala
 * @author KingkongWang
 */

#include <stdbool.h>
#include <sys/time.h>
#include <stdint.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <pthread.h>
#include <stdlib.h>


/**
 * 2019-01-01T00:00:00Z
 */
#define SNOWFLAKE_EPOC 1546272000000L 

#define WORKER_ID_BITS 5
#define DATACENTER_ID_BITS 5
#define MAX_WORKER_ID (-1L ^ (-1L << WORKER_ID_BITS))
#define MAX_DATACENTER_ID (-1L ^ (-1L << DATACENTER_ID_BITS))
#define SEQUENCE_BITS 12


#define WORKER_ID_SHIFT SEQUENCE_BITS
#define DATACENTER_ID_SHIFT (SEQUENCE_BITS + WORKER_ID_BITS)
#define TIMESTAMP_SHIFT (DATACENTER_ID_SHIFT + DATACENTER_ID_BITS)
#define SEQUENCE_MASK (-1L ^ (-1L << SEQUENCE_BITS))


typedef struct snowflake {
    int worker_id;
    int datacenter_id;
    int sequence;
    int64_t last_timestamp;
    bool initialized;
    pthread_mutex_t lock;
} snowflake_t;

// Thread-local storage for the snowflake context
static __thread snowflake_t* tls_snowflake = NULL;
static pthread_key_t snowflake_key;

// Function to clean up the thread-local snowflake context
static void cleanup_snowflake(void* arg) {
    snowflake_t* sf = (snowflake_t*)arg;
    if (sf) {
        if (sf->initialized) {
            pthread_mutex_destroy(&sf->lock);
        }
        free(sf);
    }
}

static void thread_exit_handler(void* arg) {
    (void)arg;
    cleanup_snowflake(tls_snowflake);
    tls_snowflake = NULL;
}

static int64_t time_gen() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static int64_t til_next_millis(int64_t last_timestamp) {
    int64_t ts = time_gen();
    while (ts <= last_timestamp) {
        ts = time_gen();
    }
    return ts;
}

__attribute__((constructor)) void snowflake_init_once() {
    pthread_key_create(&snowflake_key, thread_exit_handler);
}

__attribute__((destructor)) void snowflake_cleanup_once() {
    pthread_key_delete(snowflake_key);
}

bool snowflake_init(int worker_id, int datacenter_id) {
    if (tls_snowflake != NULL) {
        return false;
    }

    if (worker_id > MAX_WORKER_ID || worker_id < 0) {
        return false;
    }

    if (datacenter_id > MAX_DATACENTER_ID || datacenter_id < 0) {
        return false;
    }
    
    tls_snowflake = (snowflake_t*)malloc(sizeof(snowflake_t));
    if (tls_snowflake == NULL) {
        return false;
    }

    tls_snowflake->worker_id = worker_id;
    tls_snowflake->datacenter_id = datacenter_id;
    tls_snowflake->sequence = 0;
    tls_snowflake->last_timestamp = -1;
    tls_snowflake->initialized = false;

    if (pthread_mutex_init(&tls_snowflake->lock, NULL) != 0) {
        free(tls_snowflake);
        tls_snowflake = NULL;
        return false;
    }

    tls_snowflake->initialized = true;
    pthread_setspecific(snowflake_key, tls_snowflake);

    return true;
}

bool snowflake_next_id(char* id_str, size_t str_size) {
    int64_t ts;
    if (tls_snowflake == NULL || id_str == NULL || str_size < 21) {
        return false;
    }

    if (!tls_snowflake->initialized) {
        return false;
    }

    pthread_mutex_lock(&tls_snowflake->lock);

    ts = time_gen();
    if (ts < tls_snowflake->last_timestamp) {
        pthread_mutex_unlock(&tls_snowflake->lock);
        return false;
    }

    if (tls_snowflake->last_timestamp == ts) {
        tls_snowflake->sequence = (tls_snowflake->sequence + 1) & SEQUENCE_MASK;
        if (tls_snowflake->sequence == 0) {
            ts = til_next_millis(tls_snowflake->last_timestamp);
        }
    } else {
        tls_snowflake->sequence = 0;
    }

    tls_snowflake->last_timestamp = ts;
    ts = ((ts - SNOWFLAKE_EPOC) << TIMESTAMP_SHIFT) |
         (tls_snowflake->datacenter_id << DATACENTER_ID_SHIFT) |
         (tls_snowflake->worker_id << WORKER_ID_SHIFT) | 
         tls_snowflake->sequence;

    pthread_mutex_unlock(&tls_snowflake->lock);

    snprintf(id_str, str_size, "%lld", ts);

    return true;
}
