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
local libpq = require('libpq')
local get_result_stat = libpq.util.get_result_stat
local new_rows = require('postgres.rows').new
local new_single_rows = require('postgres.rows.single').new
--- constants
local PGRES_TUPLES_OK = libpq.PGRES_TUPLES_OK
local PGRES_SINGLE_TUPLE = libpq.PGRES_SINGLE_TUPLE

--- @class postgres.result
--- @field conn postgres.connection
--- @field res libpq.result
--- @field status integer
--- @field status_text string
local Result = {}

--- init
--- @return postgres.result
function Result:init(conn, res)
    self.conn = conn
    self.res = res
    self.status, self.status_text = res:status()
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
function Result:close()
    self.res:clear()
end

--- stat
--- @return table
function Result:stat()
    return get_result_stat(self.res)
end

--- is_null
--- @param row integer
--- @param col integer
--- @return boolean ok
function Result:is_null(row, col)
    return self.res:get_is_null(row, col)
end

--- value
--- @param row integer
--- @param col integer
--- @return string val
function Result:value(row, col)
    return self.res:get_value(row, col)
end

--- rows
--- @return postgres.rows? rows
function Result:rows()
    if self.status == PGRES_TUPLES_OK then
        return new_rows(self)
    elseif self.status == PGRES_SINGLE_TUPLE then
        return new_single_rows(self)
    end
end

--- next
--- @param deadline integer
--- @return postgres.result res
--- @return error err
--- @return boolean timeout
function Result:next(deadline)
    return self.conn:get_result(deadline)
end

return {
    new = require('metamodule').new(Result),
}

