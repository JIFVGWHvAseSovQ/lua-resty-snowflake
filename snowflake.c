/**
 * original version(scala):
 * https://github.com/twitter-archive/snowflake/blob/snowflake-2010/src/main/scala/com/twitter/service/snowflake/IdWorker.scala
 * @author KingkongWang
 */

#include <luajit-2.1/lua.h>
#include <luajit-2.1/lualib.h>
#include <luajit-2.1/lauxlib.h>
#include <sched.h>
#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/time.h>
#include <stdatomic.h>

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

#define MAX_SEQUENCE 4095
#define MAX_RETRY_COUNT 3
#define MAX_BACKWARD_MS 10
#define MAX_SPIN_COUNT 1000

typedef enum {
    SNOWFLAKE_SUCCESS = 0,
    SNOWFLAKE_ERROR_INIT_FAILED = -1,
    SNOWFLAKE_ERROR_INVALID_PARAMS = -2,
    SNOWFLAKE_ERROR_NOT_INITIALIZED = -3,
    SNOWFLAKE_ERROR_CLOCK_BACKWARDS = -4,
    SNOWFLAKE_ERROR_SEQUENCE_EXCEEDED = -5,
    SNOWFLAKE_ERROR_RETRY_EXCEEDED = -6
} snowflake_error_t;

typedef struct {
    atomic_flag flag;
    int spin_count;
} spin_lock_t;

typedef struct snowflake {
    int worker_id;
    int datacenter_id;
    atomic_int sequence;
    atomic_llong last_timestamp;
    atomic_int backward_sequence;  // 时钟回拨补偿序列
    bool initialized;
    spin_lock_t lock;
} snowflake_t;

// 自旋锁初始化
static void spin_lock_init(spin_lock_t* lock) {
    atomic_flag_clear(&lock->flag);
    lock->spin_count = 0;
}

// 自旋锁加锁
static bool spin_lock_try_lock(spin_lock_t* lock) {
    while (atomic_flag_test_and_set(&lock->flag)) {
        lock->spin_count++;
        if (lock->spin_count > MAX_SPIN_COUNT) {
            sched_yield();
            lock->spin_count = 0;
        }
    }
    return true;
}

// 自旋锁解锁
static void spin_lock_unlock(spin_lock_t* lock) {
    atomic_flag_clear(&lock->flag);
    lock->spin_count = 0;
}

static int64_t time_gen() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int64_t til_next_millis(int64_t last_timestamp) {
    int64_t ts = time_gen();
    while (ts <= last_timestamp) {
        sched_yield();
        ts = time_gen();
    }
    return ts;
}

// 初始化 Snowflake 实例（增加互斥锁初始化）
static snowflake_error_t snowflake_init(snowflake_t* context, int worker_id, int datacenter_id) {
    if(context == NULL) {
        return SNOWFLAKE_ERROR_INVALID_PARAMS;
    }

    if (worker_id > MAX_WORKER_ID || worker_id < 0) {
        return SNOWFLAKE_ERROR_INVALID_PARAMS;
    }

    if (datacenter_id > MAX_DATACENTER_ID || datacenter_id < 0) {
        return SNOWFLAKE_ERROR_INVALID_PARAMS;
    }
    
    spin_lock_init(&context->lock);
    context->worker_id = worker_id;
    context->datacenter_id = datacenter_id;
    atomic_init(&context->sequence, 0);
    atomic_init(&context->last_timestamp, -1);
    atomic_init(&context->backward_sequence, 0);
    context->initialized = true;

    return SNOWFLAKE_SUCCESS;
}

static snowflake_error_t handle_clock_backwards(snowflake_t* context, int64_t current_timestamp, int64_t last_timestamp) {
    int64_t backwards_ms = last_timestamp - current_timestamp;
    if (backwards_ms > MAX_BACKWARD_MS) {
        return SNOWFLAKE_ERROR_CLOCK_BACKWARDS;
    }

    // 在允许的回拨范围内，使用补偿序列号
    int backward_sequence = atomic_fetch_add(&context->backward_sequence, 1);
    if (backward_sequence > MAX_SEQUENCE) {
        return SNOWFLAKE_ERROR_SEQUENCE_EXCEEDED;
    }

    return SNOWFLAKE_SUCCESS;
}

static snowflake_error_t snowflake_next_id_with_retry(snowflake_t* context, char* id_str, size_t str_size) {
    if(context == NULL || id_str == NULL || str_size < 21) {
        return SNOWFLAKE_ERROR_INVALID_PARAMS;
    }

    if (!context->initialized) {
        return SNOWFLAKE_ERROR_NOT_INITIALIZED;
    }

    for (int retry = 0; retry < MAX_RETRY_COUNT; retry++) {
        int64_t current_timestamp = time_gen();
        int64_t last_timestamp;
        int sequence;
        bool need_wait = false;
        snowflake_error_t error;

        do {
            last_timestamp = atomic_load(&context->last_timestamp);

            if (current_timestamp < last_timestamp) {
                error = handle_clock_backwards(context, current_timestamp, last_timestamp);
                if (error != SNOWFLAKE_SUCCESS) {
                    if (retry == MAX_RETRY_COUNT - 1) {
                        return error;
                    }
                    sched_yield();
                    continue;
                }
            }

            if (current_timestamp == last_timestamp) {
                sequence = atomic_fetch_add(&context->sequence, 1) & SEQUENCE_MASK;
                if (sequence == 0) {
                    need_wait = true;
                    break;
                }
            } else {
                atomic_store(&context->sequence, 0);
                sequence = 0;
            }

        } while (!atomic_compare_exchange_weak(&context->last_timestamp,
                                             &last_timestamp,
                                             current_timestamp));

        if (need_wait) {
            spin_lock_try_lock(&context->lock);
            current_timestamp = til_next_millis(last_timestamp);
            atomic_store(&context->last_timestamp, current_timestamp);
            atomic_store(&context->sequence, 0);
            sequence = 0;
            spin_lock_unlock(&context->lock);
        }

        int64_t id = ((current_timestamp - SNOWFLAKE_EPOC) << TIMESTAMP_SHIFT) |
                     (context->datacenter_id << DATACENTER_ID_SHIFT) |
                     (context->worker_id << WORKER_ID_SHIFT) |
                     sequence;

        snprintf(id_str, str_size, "%lld", id);
        return SNOWFLAKE_SUCCESS;
    }

    return SNOWFLAKE_ERROR_RETRY_EXCEEDED;
}

static void snowflake_destroy(snowflake_t* context) {
    if (context != NULL) {
        context->initialized = false;
    }
}

// Lua 绑定函数：创建 Snowflake 实例
static int l_snowflake_new(lua_State* L) {
    luaL_checktype(L, 1, LUA_TNUMBER);
    luaL_checktype(L, 2, LUA_TNUMBER);
    
    int worker_id = lua_tonumber(L, 1);
    int datacenter_id = lua_tonumber(L, 2);

    snowflake_t* context = (snowflake_t*)lua_newuserdata(L, sizeof(snowflake_t));
    
    snowflake_error_t error = snowflake_init(context, worker_id, datacenter_id);
    if (error != SNOWFLAKE_SUCCESS) {
        return luaL_error(L, "Failed to initialize snowflake: error code %d", error);
    }

    luaL_getmetatable(L, "snowflake_mt");
    lua_setmetatable(L, -2);

    return 1;
}

// Lua 绑定函数：生成下一个 ID
static int l_snowflake_next_id(lua_State* L) {
    snowflake_t* context = (snowflake_t*)luaL_checkudata(L, 1, "snowflake_mt");
    
    char id_str[21];
    snowflake_error_t error = snowflake_next_id_with_retry(context, id_str, sizeof(id_str));
    
    if (error == SNOWFLAKE_SUCCESS) {
        lua_pushstring(L, id_str);
        return 1;
    }
    
    return luaL_error(L, "Failed to generate next id: error code %d", error);
}

// Lua 绑定函数：销毁 Snowflake 实例
static int l_snowflake_gc(lua_State* L) {
    snowflake_t* context = (snowflake_t*)luaL_checkudata(L, 1, "snowflake_mt");
    snowflake_destroy(context);
    return 0;
}

// 模块加载函数
int luaopen_snowflake(lua_State* L) {
    // 创建元表
    luaL_newmetatable(L, "snowflake_mt");
    
    // 设置元表的 __index 方法
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");

    // 注册方法
    lua_pushcfunction(L, l_snowflake_next_id);
    lua_setfield(L, -2, "next_id");

    // 注册析构函数（可选）
    lua_pushcfunction(L, l_snowflake_gc);
    lua_setfield(L, -2, "__gc");

    // 创建模块表
    lua_createtable(L, 0, 1);
    
    // 注册 new 函数
    lua_pushcfunction(L, l_snowflake_new);
    lua_setfield(L, -2, "new");

    return 1;
}
