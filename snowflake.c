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

// snowflake对象的ID生成方法
static int64_t generate_id(snowflake_t *sf) {
    int64_t id = -1;
    int lock_result;

    if (!sf->initialized) {
        return -1;
    }

    lock_result = pthread_mutex_lock(&sf->lock);
    if (lock_result != 0) {
        return -1;
    }

    int64_t ts = time_gen();
    
    if (ts < sf->last_timestamp) {
        pthread_mutex_unlock(&sf->lock);
        return -1;  // 时钟回拨，返回错误
    }

    if (sf->last_timestamp == ts) {
        sf->sequence = (sf->sequence + 1) & SEQUENCE_MASK;
        if (sf->sequence == 0) {
            ts = til_next_millis(sf->last_timestamp);
        }
    } else {
        sf->sequence = 0;
    }

    sf->last_timestamp = ts;
    
    id = ((ts - SNOWFLAKE_EPOC) << TIMESTAMP_SHIFT) |
         (sf->datacenter_id << DATACENTER_ID_SHIFT) |
         (sf->worker_id << WORKER_ID_SHIFT) | 
         sf->sequence;

    pthread_mutex_unlock(&sf->lock);
    
    return id;
}

// Lua对象方法：生成ID
static int l_snowflake_id(lua_State *L) {
    snowflake_t *sf = (snowflake_t *)luaL_checkudata(L, 1, "snowflake.generator");
    
    int64_t id = generate_id(sf);
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

// Lua对象的垃圾回收函数
static int l_snowflake_gc(lua_State *L) {
    snowflake_t *sf = (snowflake_t *)luaL_checkudata(L, 1, "snowflake.generator");
    if (sf->initialized) {
        pthread_mutex_destroy(&sf->lock);
        sf->initialized = false;
    }
    return 0;
}

// 初始化函数
static int l_snowflake_init(lua_State *L) {
    int worker_id = luaL_checkinteger(L, 1);
    int datacenter_id = luaL_checkinteger(L, 2);
    
    // 验证参数
    if (worker_id > MAX_WORKER_ID || worker_id < 0) {
        lua_pushnil(L);
        lua_pushstring(L, "Invalid worker_id");
        return 2;
    }

    if (datacenter_id > MAX_DATACENTER_ID || datacenter_id < 0) {
        lua_pushnil(L);
        lua_pushstring(L, "Invalid datacenter_id");
        return 2;
    }
    
    // 创建snowflake实例
    snowflake_t *sf = (snowflake_t *)lua_newuserdata(L, sizeof(snowflake_t));
    memset(sf, 0, sizeof(snowflake_t));
    
    // 初始化互斥锁
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
    
    if (pthread_mutex_init(&sf->lock, &attr) != 0) {
        pthread_mutexattr_destroy(&attr);
        lua_pushnil(L);
        lua_pushstring(L, "Failed to initialize mutex");
        return 2;
    }
    
    pthread_mutexattr_destroy(&attr);
    
    // 初始化其他字段
    sf->worker_id = worker_id;
    sf->datacenter_id = datacenter_id;
    sf->sequence = 0;
    sf->last_timestamp = -1;
    sf->initialized = true;
    
    // 设置元表
    luaL_getmetatable(L, "snowflake.generator");
    lua_setmetatable(L, -2);
    
    return 1;
}

// 注册snowflake对象的元表
static void register_snowflake_generator(lua_State *L) {
    // 创建元表
    luaL_newmetatable(L, "snowflake.generator");
    
    // 设置__gc方法
    lua_pushstring(L, "__gc");
    lua_pushcfunction(L, l_snowflake_gc);
    lua_settable(L, -3);
    
    // 创建方法表
    lua_newtable(L);
    
    lua_pushstring(L, "id");
    lua_pushcfunction(L, l_snowflake_id);
    lua_settable(L, -3);
    
    // 设置方法表为__index
    lua_setfield(L, -2, "__index");
    
    // 弹出元表
    lua_pop(L, 1);
}

// Lua模块注册
static const struct luaL_Reg snowflake[] = {
    {"init", l_snowflake_init},
    {NULL, NULL}
};

// 模块初始化函数
int luaopen_snowflake(lua_State *L) {
    // 注册snowflake生成器类型
    register_snowflake_generator(L);
    
    // 创建模块表
    luaL_newlib(L, snowflake);
    return 1;
}
