--
-- Copyright (C) 2022 Masatoshi Fukunaga
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
local format = string.format
local isa = require('isa')
local is_uint = isa.uint
local is_callable = isa.callable
local libpq = require('libpq')
--- constants
local PGRES_TUPLES_OK = libpq.PGRES_TUPLES_OK
local PGRES_SINGLE_TUPLE = libpq.PGRES_SINGLE_TUPLE

--- @class postgres.rows.single : postgres.rows
--- @field conn postgres.connection
--- @field res postgres.result
--- @field stat table
--- @field rows? postgres.rows
local SingleRows = {}

--- next
--- @param deadline integer
--- @return boolean ok
--- @return error err
--- @return boolean timeout
function SingleRows:next(deadline)
    if deadline ~= nil and not is_uint(deadline) then
        error('deadline must be uint', 2)
    end

    local stat = self.stat
    if stat.error then
        return false, stat.error
    elseif not self.res or stat.status ~= PGRES_SINGLE_TUPLE then
        return false
    end

    local res = self.res
    self:close()

    -- get next result
    local err, timeout
    res, err, timeout = res:next(deadline)
    if err or timeout then
        self.stat.error = err
        return false, err, timeout
    elseif not res then
        err = format('status is %q, but could not get the next result',
                     'PGRES_SINGLE_TUPLE')
        self.stat.error = err
        return false, err
    end

    self.res = res
    self.stat = res:stat()
    if res.status == PGRES_SINGLE_TUPLE then
        -- got next tuple
        return true
    elseif res.status == PGRES_TUPLES_OK then
        -- finish single tuple result
        self:close()
        -- get next result
        res, err, timeout = res:next(deadline)
        if err then
            self.stat.error = err
        elseif res then
            -- next rows
            self.rows = res:rows()
        end
        return false, err, timeout
    end
    return false, res.stat.error
end

--- get
--- @param decoder function
--- @return table row
--- @return error err
function SingleRows:get(decoder)
    if decoder ~= nil and not is_callable(decoder) then
        error('decoder must be function or has a __call metamethod', 2)
    end

    local stat = self.stat
    if stat.error then
        return nil, stat.error
    elseif self.res and stat.status == PGRES_SINGLE_TUPLE then
        local res = self.res
        local fields = stat.fields
        local data = {}

        -- PGRES_SINGLE_TUPLE: single tuple from larger resultset
        for col = 1, stat.nfields do
            if not res:is_null(1, col) then
                local v = res:value(1, col)
                if decoder then
                    local err
                    v, err = decoder(v, fields[col])
                    if err then
                        return nil, err
                    end
                end
                data[col] = v
            end
        end
        return data
    end
end

return {
    new = require('metamodule').new(SingleRows, 'postgres.rows'),
}

