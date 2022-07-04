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
local unpack = require('unpack')
local isa = require('isa')
local is_boolean = isa.boolean
local is_string = isa.string
local is_table = isa.table
local is_uint = isa.uint
local poll = require('gpoll')
local pollable = poll.pollable
local poll_unwait = poll.unwait
local new_result = require('postgres.result').new
--- constants
local libpq = require('libpq')
local libpq_connect = libpq.connect
--- constants
local CONNECTION_BAD = libpq.CONNECTION_BAD
-- Non-blocking mode only below here
-- PostgresPollingStatusType
local PGRES_POLLING_OK = libpq.PGRES_POLLING_OK
local PGRES_POLLING_FAILED = libpq.PGRES_POLLING_FAILED
-- these two indicate that one may use select before polling again
local PGRES_POLLING_READING = libpq.PGRES_POLLING_READING
local PGRES_POLLING_WRITING = libpq.PGRES_POLLING_WRITING

--- @class postgres.connection : postgres
--- @field conn libpq.conn
local Connection = {}

--- close
function Connection:close()
    if self.nonblock then
        poll_unwait(self.conn:socket())
    end
    self.conn:finish()
end

--- get_cancel
--- @return libpq.cancel cancel
--- @return error err
function Connection:get_cancel()
    return self.conn:get_cancel()
end

--- request_cancel
--- @return boolean ok
--- @return error err
function Connection:request_cancel()
    return self.conn:request_cancel()
end

--- status
--- @return integer status
function Connection:status()
    return self.conn:status()
end

--- transaction_status
--- @return integer status
function Connection:transaction_status()
    return self.conn:transaction_status()
end

--- parameter_status
--- @param param_name? string
--- @return string status
function Connection:parameter_status(param_name)
    return self.conn:parameter_status(param_name)
end

--- protocol_version
--- @return integer version
function Connection:protocol_version()
    return self.conn:protocol_version()
end

--- server_version
--- @return integer version
function Connection:server_version()
    return self.conn:server_version()
end

--- error_message
--- @return string errmsg
function Connection:error_message()
    return self.conn:error_message()
end

--- backend_pid
--- @return integer pid
function Connection:backend_pid()
    return self.conn:backend_pid()
end

--- pipeline_status
--- @return integer status
function Connection:pipeline_status()
    return self.conn:pipeline_status()
end

--- connection_needs_password
--- @return boolean ok
function Connection:connection_needs_password()
    return self.conn:connection_needs_password()
end

--- connection_used_password
--- @return boolean ok
function Connection:connection_used_password()
    return self.conn:connection_used_password()
end

--- client_encoding
--- @return string encoding
function Connection:client_encoding()
    return self.conn:client_encoding()
end

--- set_client_encoding
--- @param encoding string
--- @return boolean ok
--- @return error err
function Connection:set_client_encoding(encoding)
    return self.conn:set_client_encoding(encoding)
end

--- ssl_in_use
--- @return boolean ok
function Connection:ssl_in_use(encoding)
    return self.conn:ssl_in_use()
end

--- ssl_attribute
--- @param attr_name string
--- @return string attr
function Connection:ssl_attribute(attr_name)
    return self.conn:ssl_attribute(attr_name)
end

--- ssl_attribute_names
--- @return string[] attr_names
function Connection:ssl_attribute_names()
    return self.conn:ssl_attribute_names()
end

--- set_error_verbosity
--- @param verbosity integer
--- @return integer verbosity
function Connection:set_error_verbosity(verbosity)
    return self.conn:set_error_verbosity(verbosity)
end

--- set_error_context_visibility
--- @param visibility integer
--- @return integer visibility
function Connection:set_error_context_visibility(visibility)
    return self.conn:set_error_context_visibility(visibility)
end

--- set_notice
--- @param fn function
function Connection:set_notice_processor(fn)
    self.conn:set_notice_processor(fn)
end

--- set_notice_receiver
--- @param fn function
function Connection:set_notice_receiver(fn)
    self.conn:set_notice_receiver(fn)
end

--- call_notice_processor
--- @param msg string
--- @return boolean ok
function Connection:call_notice_processor(msg)
    return self.conn:call_notice_processor(msg)
end

--- call_notice_receiver
--- @param res libpq.result
--- @return boolean ok
function Connection:call_notice_receiver(res)
    return self.conn:call_notice_receiver(res)
end

--- trace
--- @param f file*
--- @return file* f
function Connection:trace(f)
    return self.conn:trace(f)
end

--- untrace
--- @return file* f
function Connection:untrace()
    return self.conn:untrace()
end

--- set_trace_flags
--- @vararg integer
function Connection:set_trace_flags(...)
    self.conn:set_trace_flags(...)
end

--- flush
--- @param deadline integer
--- @return boolean ok
--- @return error err
--- @return boolean timeout
function Connection:flush(deadline)
    while true do
        local ok, err, again = self.conn:flush()
        if not again then
            return ok, err
        end

        local timeout
        ok, err, timeout = self:wait_writable(deadline)
        if not ok then
            return false, err, timeout
        end
    end
end

--- query
--- @param query string
--- @param params any[]
--- @param deadline integer
--- @param single_row_mode boolean
--- @return postgres.result res
--- @return error err
--- @return boolean timeout
function Connection:query(query, params, deadline, single_row_mode)
    if not is_string(query) then
        error('query must be string', 2)
    elseif params ~= nil and not is_table(params) then
        error('params must be table', 2)
    elseif deadline ~= nil and not is_uint(deadline) then
        error('deadline must be uint', 2)
    elseif single_row_mode ~= nil and not is_boolean(single_row_mode) then
        error('single_row_mode must be boolean', 2)
    end

    local ok, err
    if not params or #params == 0 then
        ok, err = self.conn:send_query(query)
    else
        ok, err = self.conn:send_query_params(query, unpack(params))
    end
    if not ok then
        return nil, err
    end

    local timeout
    ok, err, timeout = self:flush(deadline)
    if not ok then
        return nil, err, timeout
    elseif single_row_mode then
        assert(self.conn:set_single_row_mode(), 'failed to set single row mode')
    end

    return self:get_result(deadline)
end

--- get_result
--- @param deadline integer
--- @return postgres.result res
--- @return error err
--- @return boolean timeout
function Connection:get_result(deadline)
    if deadline ~= nil and not is_uint(deadline) then
        error('deadline must be uint', 2)
    end

    while true do
        local busy, err = self.conn:is_busy()
        if err then
            return nil, err
        elseif not busy then
            local res
            res, err = self.conn:get_result()
            if not res then
                return nil, err
            end
            return new_result(self, res)
        end

        -- wait until readable
        local ok, werr, timeout = self:wait_readable(deadline)
        if not ok then
            return nil, werr, timeout
        end
    end
end

--- make_empty_result
--- @param status integer
--- @return postgres.result res
function Connection:make_empty_result(status)
    return self.conn:make_empty_result(status)
end

Connection = require('metamodule').new(Connection, 'postgres')

--- connect
--- @param conninfo string
--- @param deadline integer
--- @return postgres.connection
--- @return error
local function new(conninfo, deadline)
    if deadline ~= nil and not is_uint(deadline) then
        error('deadline must be uint', 2)
    end

    local is_pollable = pollable()
    local is_nonblock = is_pollable or deadline ~= nil
    local conn, err = libpq_connect(conninfo, is_nonblock)
    if err then
        return nil, err
    end

    -- check status
    local status = conn:status()
    if status == CONNECTION_BAD then
        return nil, conn:error_message()
    end

    -- sync connect
    if not is_nonblock then
        return Connection(conn)
    end

    -- async connect
    local c = Connection(conn)
    -- check status
    status = conn:connect_poll()
    if status == PGRES_POLLING_OK then
        return c
    elseif status == PGRES_POLLING_FAILED then
        return nil, conn:error_message()
    end

    -- polling a status
    local ok, timeout
    if status == PGRES_POLLING_READING then
        ok, err, timeout = c:wait_readable(deadline)
    elseif status == PGRES_POLLING_WRITING then
        ok, err, timeout = c:wait_writable(deadline)
    else
        return nil, format('got unsupported status: %d', status)
    end
    -- got error or timeout
    if not ok then
        conn:finish()
        return nil, err, timeout
    end

    -- check status
    if conn:connect_poll() ~= PGRES_POLLING_OK then
        return nil, conn:error_message()
    end
    return c
end

return {
    new = new,
}
