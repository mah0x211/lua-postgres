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
local setmetatable = setmetatable
local isa = require('isa')
local is_callable = isa.callable
local is_uint = isa.uint

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
    local pool = self.pools[conn.conninfo]
    if not pool then
        pool = setmetatable({}, {
            __mode = 'k',
        })
        self.pools[conn.conninfo] = pool
    end
    pool[conn] = true
end

--- get
--- @param conninfo string?
--- @return postgres.connection? conn
function Pool:get(conninfo)
    if conninfo == nil then
        conninfo = ''
    end

    local pool = self.pools[conninfo]
    if pool then
        local conn = next(pool)
        if conn then
            pool[conn] = nil
            return conn
        end
    end
end

--- default_pool_clear_callback
--- @return boolean ok
local function default_pool_clear_callback()
    return true
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
    for conninfo, pool in pairs(self.pools) do
        local conn = next(pool)
        while conn do
            local ok, res, err = pcall(callback, conninfo, conn)
            if not ok then
                return nconn, res
            elseif res == true then
                -- close
                pool[conn] = nil
                conn:close()
                nconn = nconn + 1
                if n > 0 and nconn >= n then
                    return nconn
                end
                conn = nil
            elseif err then
                return nconn, err
            end
            conn = next(pool, conn)
        end
    end

    return nconn
end

return {
    new = require('metamodule').new(Pool),
}

