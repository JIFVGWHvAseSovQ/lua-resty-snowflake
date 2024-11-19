/**
 * original version(scala):
 * https://github.com/twitter-archive/snowflake/blob/snowflake-2010/src/main/scala/com/twitter/service/snowflake/IdWorker.scala
 * @author KingkongWang
 */

#include <luajit-2.1/lua.h>
#include <luajit-2.1/lauxlib.h>
#include <luajit-2.1/lualib.h>
#include <stdbool.h>
#include <sys/time.h>
#include <stdint.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>

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

typedef struct snowflake {
    int worker_id;
    int datacenter_id;
    int sequence;
    int64_t last_timestamp;
    bool initialized;
    pthread_mutex_t mutex;  // 添加互斥锁
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

// 初始化 Snowflake 实例（增加互斥锁初始化）
static bool snowflake_init(snowflake_t* context, int worker_id, int datacenter_id) {
    if(context == NULL) {
        return false;
    }

    if (worker_id > MAX_WORKER_ID || worker_id < 0) {
        return false;
    }

    if (datacenter_id > MAX_DATACENTER_ID || datacenter_id < 0) {
        return false;
    }
    
    // 初始化互斥锁
    if (pthread_mutex_init(&context->mutex, NULL) != 0) {
        return false;
    }
    
    context->worker_id = worker_id;
    context->datacenter_id = datacenter_id;
    context->sequence = 0;
    context->last_timestamp = -1;
    context->initialized = true;

    return true;
}

// 线程安全的 ID 生成函数
static bool snowflake_next_id(snowflake_t* context, char* id_str, size_t str_size) {
    if(context == NULL || id_str == NULL || str_size < 21) {
        return false;
    }

    // 加锁
    pthread_mutex_lock(&context->mutex);

    if (!context->initialized) {
        pthread_mutex_unlock(&context->mutex);
        return false;
    }

    int64_t ts = time_gen();
    if (ts < context->last_timestamp) {
        pthread_mutex_unlock(&context->mutex);
        return false;
    }

    if (context->last_timestamp == ts) {
        context->sequence = (context->sequence + 1) & SEQUENCE_MASK;
        if (context->sequence == 0) {
            ts = til_next_millis(context->last_timestamp);
        }
    } else {
        context->sequence = 0;
    }

    context->last_timestamp = ts;
    int64_t id = ((ts - SNOWFLAKE_EPOC) << TIMESTAMP_SHIFT) |
            (context->datacenter_id << DATACENTER_ID_SHIFT) |
            (context->worker_id << WORKER_ID_SHIFT) | 
            context->sequence;

    // 生成 ID 后立即解锁
    snprintf(id_str, str_size, "%lld", id);
    pthread_mutex_unlock(&context->mutex);

    return true;
}

// 资源清理函数（增加互斥锁销毁）
static void snowflake_destroy(snowflake_t* context) {
    if (context != NULL) {
        pthread_mutex_destroy(&context->mutex);
    }
}

// Lua 绑定函数：创建 Snowflake 实例
static int lua_snowflake_new(lua_State* L) {
    // 检查参数个数和类型
    luaL_checktype(L, 1, LUA_TNUMBER);  // worker_id
    luaL_checktype(L, 2, LUA_TNUMBER);  // datacenter_id
    
    int worker_id = lua_tonumber(L, 1);
    int datacenter_id = lua_tonumber(L, 2);

    // 参数验证
    if (worker_id < 0 || worker_id > MAX_WORKER_ID || 
        datacenter_id < 0 || datacenter_id > MAX_DATACENTER_ID) {
        return luaL_error(L, "worker_id and datacenter_id must be between 0 and 31");
    }

    // 分配 Snowflake 实例
    snowflake_t* context = (snowflake_t*)lua_newuserdata(L, sizeof(snowflake_t));
    
    // 初始化实例
    if (!snowflake_init(context, worker_id, datacenter_id)) {
        return luaL_error(L, "Failed to initialize snowflake");
    }

    // 设置元表
    luaL_getmetatable(L, "snowflake_mt");
    lua_setmetatable(L, -2);

    return 1;
}

// Lua 绑定函数：生成下一个 ID
static int lua_snowflake_next_id(lua_State* L) {
    snowflake_t* context = (snowflake_t*)luaL_checkudata(L, 1, "snowflake_mt");
    
    char id_str[21];
    if (snowflake_next_id(context, id_str, sizeof(id_str))) {
        lua_pushstring(L, id_str);
        return 1;
    }
    
    return luaL_error(L, "Failed to generate next id");
}

// Lua 绑定函数：销毁 Snowflake 实例
static int lua_snowflake_destroy(lua_State* L) {
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
    lua_pushcfunction(L, lua_snowflake_next_id);
    lua_setfield(L, -2, "next_id");

    // 注册析构函数（可选）
    lua_pushcfunction(L, lua_snowflake_destroy);
    lua_setfield(L, -2, "__gc");

    // 创建模块表
    lua_createtable(L, 0, 1);
    
    // 注册 new 函数
    lua_pushcfunction(L, lua_snowflake_new);
    lua_setfield(L, -2, "new");

    return 1;
}
