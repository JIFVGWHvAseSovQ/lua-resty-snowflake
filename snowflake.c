#include <stdbool.h>
#include <sys/time.h>
#include <stdint.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <pthread.h>
#include <stdlib.h>

// ... (previous #define statements remain unchanged)

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

// Function to clean up the thread-local snowflake context
static void cleanup_tls_snowflake(void* arg) {
    (void)arg; // Unused parameter
    if (tls_snowflake) {
        if (tls_snowflake->initialized) {
            pthread_mutex_destroy(&tls_snowflake->lock);
        }
        free(tls_snowflake);
        tls_snowflake = NULL;
    }
}

// ... (previous static functions time_gen and til_next_millis remain unchanged)

bool snowflake_init(int worker_id, int datacenter_id) {
    if (tls_snowflake != NULL) {
        // Already initialized for this thread
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
    tls_snowflake->initialized = true;

    if (pthread_mutex_init(&tls_snowflake->lock, NULL) != 0) {
        free(tls_snowflake);
        tls_snowflake = NULL;
        return false;
    }

    // Register the cleanup function
    pthread_cleanup_push(cleanup_tls_snowflake, NULL);

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

// Optional: Explicit cleanup function if needed
void snowflake_cleanup(void) {
    cleanup_tls_snowflake(NULL);
    pthread_cleanup_pop(0);
}
