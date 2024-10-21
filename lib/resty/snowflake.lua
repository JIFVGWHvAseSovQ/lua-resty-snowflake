local ffi = require "ffi"
local ffi_load = ffi.load
local ffi_new = ffi.new
local ffi_string = ffi.string
local semaphore = require("ngx.semaphore")
local ok, sf = pcall(ffi_load, "snowflake")
assert(ok, sf)
local lock = semaphore.new(20)

ffi.cdef[[
    typedef struct snowflake{
     
        int worker_id;
        int datacenter_id;
        int sequence;
        int64_t last_timestamp;
        bool initialized;
    }snowflake_t;

    bool snowflake_init(snowflake_t*, int, int);
    bool snowflake_next_id(snowflake_t*, char*, size_t);
]]

local _M = {_VERSION = '0.0.1'}
local mt = { __index = _M }

function _M.new(self, worker_id, datacenter_id)
    assert(worker_id >= 0 and worker_id <= 0x1f)
    assert(datacenter_id >= 0 and datacenter_id <= 0x1f)

    local snowflake = ffi_new("snowflake_t")
    local flag = sf.snowflake_init(snowflake, worker_id, datacenter_id)
    if not flag then
        return nil
    end

    return setmetatable({ 
        context = snowflake
    }, mt)

end

function _M.next_id(self)
    lock:wait(0)
    --local id = ffi_new("int64_t[1]")
    --local ok = sf.snowflake_next_id(self.context, id)
    local id_buf = ffi_new("char[21]")
    local ok = sf.snowflake_next_id(self.context, id_buf, 21)
    assert(ok)
    lock:post()

    return ffi_string(id_buf)
    --return id[0]
end

return _M
