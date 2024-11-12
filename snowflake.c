/**
 * original version(scala):
 * https://github.com/twitter-archive/snowflake/blob/snowflake-2010/src/main/scala/com/twitter/service/snowflake/IdWorker.scala
 * @author KingkongWang
 */

#include <luajit-2.1/lua.h>
#include <luajit-2.1/lauxlib.h>
#include <stdbool.h>
#include <sys/time.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>


/**
 * 2019-01-01T00:00:00Z
 * #define SNOWFLAKE_EPOC 1546272000000L
 * 2023-10-01 00:00:00 UTC
 * #define SNOWFLAKE_EPOC 1696118400000L
 */
#define SNOWFLAKE_EPOC 1696118400000L

#define WORKER_ID_BITS 5
#define DATACENTER_ID_BITS 5
#define MAX_WORKER_ID (-1L ^ (-1L << WORKER_ID_BITS))
#define MAX_DATACENTER_ID (-1L ^ (-1L << DATACENTER_ID_BITS))
#define SEQUENCE_BITS 12


#define WORKER_ID_SHIFT SEQUENCE_BITS
#define DATACENTER_ID_SHIFT (SEQUENCE_BITS + WORKER_ID_BITS)
#define TIMESTAMP_SHIFT (DATACENTER_ID_SHIFT + DATACENTER_ID_BITS)
#define SEQUENCE_MASK (-1L ^ (-1L << SEQUENCE_BITS))

typedef struct {
    int worker_id;
    int datacenter_id;
    int sequence;
    int64_t last_timestamp;
    pthread_mutex_t lock;
    bool initialized;
} snowflake_t;

// 全局context，用于存储snowflake状态
static snowflake_t g_context = {
    .worker_id = 0,
    .datacenter_id = 0,
    .sequence = 0,
    .last_timestamp = -1,
    .initialized = false
};

static int64_t time_gen() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

static int64_t til_next_millis(int64_t last_timestamp) {
    int64_t ts = time_gen();
    while (ts < last_timestamp) {
        ts = time_gen();
    }

    return ts;
}

// 初始化函数
static bool snowflake_init(int worker_id, int datacenter_id) {
    if (worker_id > MAX_WORKER_ID || worker_id < 0) {
        return false;
    }

    if (datacenter_id > MAX_DATACENTER_ID || datacenter_id < 0) {
        return false;
    }

    if (!g_context.initialized) {
        pthread_mutexattr_t attr;
        pthread_mutexattr_init(&attr);
        pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
        
        if (pthread_mutex_init(&g_context.lock, &attr) != 0) {
            pthread_mutexattr_destroy(&attr);
            return false;
        }
        
        pthread_mutexattr_destroy(&attr);
        g_context.initialized = true;
    }
    
    g_context.worker_id = worker_id;
    g_context.datacenter_id = datacenter_id;
    g_context.sequence = 0;
    g_context.last_timestamp = -1;

    return true;
}

// 生成下一个ID
static int64_t snowflake_next_id() {
    int64_t id = -1;
    int lock_result;

    if (!g_context.initialized) {
        return -1;
    }

    // 获取互斥锁
    lock_result = pthread_mutex_lock(&g_context.lock);
    if (lock_result != 0) {
        return -1;
    }

    int64_t ts = time_gen();
    
    if (ts < g_context.last_timestamp) {
        pthread_mutex_unlock(&g_context.lock);
        return -1;  // 时钟回拨，返回错误
    }

    if (g_context.last_timestamp == ts) {
        g_context.sequence = (g_context.sequence + 1) & SEQUENCE_MASK;
        if (g_context.sequence == 0) {
            ts = til_next_millis(g_context.last_timestamp);
        }
    } else {
        g_context.sequence = 0;
    }

    g_context.last_timestamp = ts;
    
    id = ((ts - SNOWFLAKE_EPOC) << TIMESTAMP_SHIFT) |
         (g_context.datacenter_id << DATACENTER_ID_SHIFT) |
         (g_context.worker_id << WORKER_ID_SHIFT) | 
         g_context.sequence;

    // 释放互斥锁
    pthread_mutex_unlock(&g_context.lock);
    
    return id;
}

// 清理函数
static void snowflake_cleanup() {
    if (g_context.initialized) {
        pthread_mutex_destroy(&g_context.lock);
        g_context.initialized = false;
    }
}

// Lua接口函数
static int l_snowflake_id(lua_State *L) {
    int worker_id = luaL_checkinteger(L, 1);
    int datacenter_id = luaL_checkinteger(L, 2);
    
    // 如果worker_id或datacenter_id发生变化，重新初始化
    if (!g_context.initialized || 
        worker_id != g_context.worker_id || 
        datacenter_id != g_context.datacenter_id) {
        
        if (!snowflake_init(worker_id, datacenter_id)) {
            lua_pushnil(L);
            lua_pushstring(L, "Failed to initialize snowflake");
            return 2;
        }
    }
    
    int64_t id = snowflake_next_id();
    if (id == -1) {
        lua_pushnil(L);
        lua_pushstring(L, "Failed to generate ID");
        return 2;
    }
    
    char id_str[21];
    snprintf(id_str, sizeof(id_str), "%lld", id);
    lua_pushstring(L, id_str);
    return 1;
}

// Lua模块注册
static const struct luaL_Reg snowflake[] = {
    {"id", l_snowflake_id},
    {NULL, NULL}
};

// 模块初始化函数
int luaopen_snowflake(lua_State *L) {
    luaL_newlib(L, snowflake);
    return 1;
}

// 模块卸载函数 
__attribute__((destructor)) static void module_cleanup(void) {
    snowflake_cleanup();
}
