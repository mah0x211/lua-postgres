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
local isa = require('isa')
local is_uint = isa.uint
local is_callable = isa.callable
local libpq = require('libpq')
--- constants
local PGRES_TUPLES_OK = libpq.PGRES_TUPLES_OK

--- @class postgres.rows
--- @field res postgres.result
--- @field stat table
--- @field rows? postgres.rows
--- @field rowidx integer
local Rows = {}

--- init
--- @return postgres.rows
function Rows:init(res)
    self.res = res
    self.stat = res:stat()
    self.rowidx = 1
    -- PGRES_TUPLES_OK:         a query command that returns tuples was executed
    --                          properly by the backend, PGresult contains the
    --                          result tuples.
    -- PGRES_SINGLE_TUPLE:      single tuple from larger resultset
    -- PGRES_EMPTY_QUERY:       empty query string was executed.
    -- PGRES_COMMAND_OK:        a query command that doesn't return anything was
    --                          executed properly by the backend.
    -- PGRES_COPY_OUT:          Copy Out data transfer in progress.
    -- PGRES_COPY_IN:           Copy In data transfer in progress.
    -- PGRES_BAD_RESPONSE:      an unexpected response was recv'd from the
    --                          backend.
    -- PGRES_NONFATAL_ERROR:    notice or warning message.
    -- PGRES_FATAL_ERROR:       query failed.
    -- PGRES_COPY_BOTH:         Copy In/Out data transfer in progress.
    -- PGRES_PIPELINE_SYNC:     pipeline synchronization point.
    -- PGRES_PIPELINE_ABORTED:  Command didn't run because of an abort earlier
    --                          in a pipeline.
    return self
end

--- close
function Rows:close()
    if self.res then
        self.res:close()
        self.res = nil
    end
end

--- next
--- @param deadline integer
--- @return boolean ok
--- @return error err
--- @return boolean timeout
function Rows:next(deadline)
    if deadline ~= nil and not is_uint(deadline) then
        error('deadline must be uint', 2)
    end

    local stat = self.stat
    if stat.error then
        return false, stat.error
    elseif not self.res or stat.status ~= PGRES_TUPLES_OK then
        return false
    end

    -- set to next row index
    self.rowidx = self.rowidx + 1
    if self.rowidx <= stat.ntuples then
        return true
    end

    local res = self.res
    self:close()

    -- get next result
    local err, timeout
    res, err, timeout = res:next(deadline)
    if err or timeout then
        self.stat.error = err
        return false, err, timeout
    elseif res then
        -- next rows
        self.rows = res:rows()
    end
    return false
end

--- next_rows
--- @return postgres.rows rows
function Rows:next_rows()
    if self.rows then
        local rows = self.rows
        self.rows = nil
        return rows
    end
end

--- get
--- @param decoder function
--- @return table row
--- @return error err
function Rows:get(decoder)
    if decoder ~= nil and not is_callable(decoder) then
        error('decoder must be function or has a __call metamethod', 2)
    end

    local stat = self.stat
    if stat.error then
        return nil, stat.error
    elseif self.res and stat.status == PGRES_TUPLES_OK and self.rowidx > 0 then
        local res = self.res
        local fields = stat.fields
        local rowidx = self.rowidx
        local data = {}

        -- PGRES_TUPLES_OK: a query command that returns tuples was executed
        --                  properly by the backend, PGresult contains the
        --                  result tuples.
        for col = 1, stat.nfields do
            if not res:is_null(rowidx, col) then
                local v = res:value(rowidx, col)
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

--- getall
--- @param decoder? function
--- @param deadline? integer
--- @return table[]? rows
--- @return error? err
--- @return boolean? timeout
function Rows:getall(decoder, deadline)
    local list = {}
    repeat
        local row, err = self:get(decoder)
        if not row then
            return nil, err
        end
        list[#list + 1] = row

        local ok, timeout
        ok, err, timeout = self:next(deadline)
        if err or timeout then
            return nil, err, timeout
        end
    until ok == false

    return list
end

return {
    new = require('metamodule').new(Rows),
}

