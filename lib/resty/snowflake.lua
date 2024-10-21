local ffi = require "ffi"
local ffi_load = ffi.load
local ffi_new = ffi.new
local ffi_string = ffi.string
local ok, sf = pcall(ffi_load, "snowflake")
assert(ok, sf)

ffi.cdef[[
    bool snowflake_init(int, int);
    bool snowflake_next_id(char*, size_t);
]]

local _M = {_VERSION = '0.0.2'}
local mt = { __index = _M }

function _M.new(self, worker_id, datacenter_id)
    assert(worker_id >= 0 and worker_id <= 0x1f)
    assert(datacenter_id >= 0 and datacenter_id <= 0x1f)

    local flag = sf.snowflake_init(worker_id, datacenter_id)
    if not flag then
        return nil
    end

    return setmetatable({}, mt)
end

function _M.next_id(self)
    local id_buf = ffi_new("char[21]")
    local ok = sf.snowflake_next_id(id_buf, 21)
    assert(ok)

    return ffi_string(id_buf)
end

return _M

