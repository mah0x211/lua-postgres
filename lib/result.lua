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
local new_rows = require('postgres.rows').new
local new_single_rows = require('postgres.rows.single').new
local get_result_stat = require('postgres.util').get_result_stat

--- @class postgres.result
--- @field conn postgres.connection
--- @field res postgres.pgresult
--- @field res_stat? table
--- @field is_cleared? boolean
local Result = {}

--- init
--- @param conn postgres.connection
--- @param res postgres.pgresult
--- @return postgres.result
function Result:init(conn, res)
    self.conn = conn
    self.res = res
    return self
end

--- next
--- @param sec? number
--- @return postgres.result? res
--- @return any err
--- @return boolean? timeout
function Result:next(sec)
    return self.conn:get_result(sec)
end

--- clear
function Result:clear()
    self.res:clear()
    self.is_cleared = true
end

--- close
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Result:close()
    local res = self
    local err, timeout
    while res do
        res:clear()
        res, err, timeout = self:next()
    end

    if err or timeout then
        return false, err, timeout
    end

    return true
end

--- status
--- @return string status
function Result:status()
    return self:stat().status
end

--- stat
--- @return table
function Result:stat()
    if not self.res_stat then
        self.res_stat = get_result_stat(self.res)
    end
    return self.res_stat
end

--- value
--- @param row integer
--- @param col integer
--- @return string val
function Result:value(row, col)
    return self.res:get_value(row, col)
end

--- rowinfo
--- @return integer? status
--- @return integer? nrow
function Result:rowinfo()
    local stat = self:stat()
    if stat.status == 'tuples_ok' or stat.status == 'single_tuple' then
        return stat.status, stat.ntuples
    end
end

--- rows
--- @return postgres.rows? rows
--- @return any err
function Result:rows()
    if self.is_cleared then
        return nil
    end

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
    local stat = self:stat()
    if stat.error then
        return nil, stat.error
    elseif stat.status == 'tuples_ok' then
        if stat.ntuples > 0 then
            return new_rows(self, stat.ntuples, stat.fields)
        end
    elseif stat.status == 'single_tuple' then
        if stat.ntuples > 0 then
            return new_single_rows(self, stat.ntuples, stat.fields)
        end
    end
end

return {
    new = require('metamodule').new(Result),
}

