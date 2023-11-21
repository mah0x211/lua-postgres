--
-- Copyright (C) 2023 Masatoshi Fukunaga
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
-- THE SOFTWARE.
--
--- assign to local
local next = next
local pairs = pairs
local pcall = pcall
local select = select
local type = type
local floor = math.floor
local getmetatable = debug.getmetatable
local setmetatable = setmetatable
local instanceof = require('metamodule').instanceof
local parse_conninfo = require('postgres.conninfo')

--- @class postgres.pool
--- @field pools table<string, table<postgres.connection, boolean>>
local Pool = {}

function Pool:init()
    self.pools = {}
    return self
end

--- set
--- @param conn postgres.connection
function Pool:set(conn)
    assert(instanceof(conn, 'postgres.connection'),
           'conn must be postgres.connection')
    local conninfo = conn:get_conninfo()
    local pool = self.pools[conninfo]

    if not pool then
        -- create new pool for the conninfo
        pool = setmetatable({}, {
            __mode = 'k',
        })
        self.pools[conninfo] = pool
    end
    pool[conn] = true
end

--- get
--- @param conninfo string?
--- @return postgres.connection? conn
function Pool:get(conninfo)
    if conninfo == nil then
        conninfo = select(3, parse_conninfo(''))
    end

    local pool = self.pools[conninfo]
    local conn = pool and next(pool)
    if conn then
        pool[conn] = nil
        return conn
    end
end

--- default_pool_clear_callback
--- @return boolean ok
local function default_pool_clear_callback()
    return true
end

local INF_POS = math.huge

--- is_uint
--- @param v any
--- @return boolean
local function is_uint(v)
    return type(v) == 'number' and (v < INF_POS and v >= 0) and floor(v) == v
end

--- is_callable
--- @param v any
--- @return boolean
local function is_callable(v)
    if type(v) == 'function' then
        return true
    end

    local mt = getmetatable(v)
    return type(mt) == 'table' and type(mt.__call) == 'function'
end

--- clear
--- @param callback fun(conninfo:string):(ok:boolean, err:any)
--- @param n integer
--- @return integer n
--- @return any err
function Pool:clear(callback, n)
    callback = callback or default_pool_clear_callback
    n = n or 0

    if not is_callable(callback) then
        error('callback must be callable', 2)
    elseif not is_uint(n) then
        error('n must be uint', 2)
    end

    local nconn = 0
    local errs = {}
    for conninfo, pool in pairs(self.pools) do
        local conn = next(pool)
        while conn do
            local ok, res, err = pcall(callback, conninfo, conn)
            if not ok then
                errs[#errs + 1] = res
            elseif res == true then
                -- close
                pool[conn] = nil
                conn:close()
                nconn = nconn + 1
                if n > 0 and nconn >= n then
                    return nconn, #errs > 0 and errs or nil
                end
                conn = nil
            elseif err then
                errs[#errs + 1] = err
            end
            conn = next(pool, conn)
        end
    end

    return nconn, #errs > 0 and errs or nil
end

return {
    new = require('metamodule').new(Pool),
}

