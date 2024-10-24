local ffi = require "ffi"
local ffi_load = ffi.load
local ffi_new = ffi.new
local ffi_string = ffi.string
local ok, sf = pcall(ffi_load, "snowflake")
assert(ok, sf)

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
    -- 改用 pcall 来捕获断言错误
    local ok, err = pcall(function()
        assert(worker_id >= 0 and worker_id <= 0x1f,
            string.format("worker_id must be between 0 and %d", 0x1f)
        )
        assert(datacenter_id >= 0 and datacenter_id <= 0x1f,
            string.format("datacenter_id must be between 0 and %d", 0x1f)
        )
    end)
    if not ok then
        return nil, err
    end

    local snowflake = ffi_new("snowflake_t")
    local flag = sf.snowflake_init(snowflake, worker_id, datacenter_id)
    if not flag then
        return nil, "failed to initialize snowflake"
    end

    return setmetatable({ 
        context = snowflake
    }, mt)

end

function _M.next_id(self)
    --local id = ffi_new("int64_t[1]")
    --local ok = sf.snowflake_next_id(self.context, id)
    local id_buf = ffi_new("char[21]")
    local ok = sf.snowflake_next_id(self.context, id_buf, 21)
    assert(ok, "failed to generate next id")

    return ffi_string(id_buf)
    --return id[0]
end

return _M
