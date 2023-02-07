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
local gsub = string.gsub
local concat = table.concat
local pcall = pcall
local type = type
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
--- @return any err
function Connection:get_cancel()
    return self.conn:get_cancel()
end

--- request_cancel
--- @return boolean ok
--- @return any err
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
--- @return any err
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

--- escape_string
--- @param str string
--- @return string? str
--- @return any err
function Connection:escape_string_conn(str)
    return self.conn:escape_string_conn(str)
end

--- escape_literal
--- @param str string
--- @return string? str
--- @return any err
function Connection:escape_literal(str)
    return self.conn:escape_literal(str)
end

--- escape_identifier
--- @param str string
--- @return string? str
--- @return any err
function Connection:escape_identifier(str)
    return self.conn:escape_identifier(str)
end

--- escape_bytea_conn
--- @param str string
--- @return string? str
--- @return any err
function Connection:escape_bytea_conn(str)
    return self.conn:escape_bytea_conn(str)
end

--- encrypt_password_conn
--- @param passwd string
--- @param user string
--- @param algorithm string
--- @return string? str
--- @return any err
function Connection:encrypt_password_conn(passwd, user, algorithm)
    return self.conn:encrypt_password_conn(passwd, user, algorithm)
end

--- flush
--- @param deadline integer
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
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

--- stringify
--- @param v any
--- @return any v
--- @return any err
local function stringify(v)
    local t = type(v)
    if t == 'string' then
        return v, t
    elseif t == 'nil' then
        return 'NULL', t
    elseif t == 'boolean' then
        return v and 'TRUE' or 'FALSE', t
    elseif t == 'number' then
        return tostring(v)
    elseif t == 'table' then
        return v, t
    end
    return nil, t
end

--- replace_named_params
--- @param query string
--- @param params table
--- @return string? query
--- @return table? params
--- @return any err
function Connection:replace_named_params(query, params)
    if not is_string(query) then
        error('query must be string', 2)
    elseif not is_table(params) then
        error('params must be table', 2)
    end

    local newparams = {
        unpack(params),
    }
    local param_ids = {}
    local ok, res = pcall(gsub, query, '%${([^}]+)}', function(name)
        if param_ids[name] then
            return param_ids[name]
        end

        -- convert to positional parameters
        local v, t = stringify(params[name])

        if not v then
            error(format('invalid parameter %q: data type %q is not supported',
                         name, t))
        elseif t ~= 'table' then
            -- add positional parameter
            newparams[#newparams + 1] = v
            param_ids[name] = '$' .. #newparams
            return param_ids[name]
        end

        -- convert table to array
        local stack = {}
        local ctx = {
            ids = {},
            tbl = v,
        }
        ctx.idx, v = next(ctx.tbl)
        while v do
            v, t = stringify(v)
            if not v then
                error(format(
                          'invalid parameter %q: data type %q is not supported',
                          name, t))
            elseif t ~= 'table' then
                -- convert to positional parameters
                newparams[#newparams + 1] = v
                ctx.ids[#ctx.ids + 1] = '$' .. #newparams
            else
                stack[#stack + 1] = ctx
                ctx = {
                    ids = {},
                    tbl = v,
                }
            end

            ctx.idx, v = next(ctx.tbl, ctx.idx)
            if not v then
                while #stack > 0 do
                    local child = ctx
                    ctx, stack[#stack] = stack[#stack], nil
                    ctx.ids[#ctx.ids + 1] =
                        '{' .. concat(child.ids, ', ') .. '}'
                    ctx.idx, v = next(ctx.tbl, ctx.idx)
                    if v then
                        break
                    end
                end
            end
        end

        param_ids[name] = concat(ctx.ids, ', ')
        return param_ids[name]
    end)

    if not ok then
        return nil, nil, res
    end

    return res, newparams
end

--- query
--- @param query string
--- @param params? table?
--- @param deadline integer
--- @param single_row_mode boolean
--- @return postgres.result? res
--- @return any err
--- @return boolean? timeout
function Connection:query(query, params, deadline, single_row_mode)
    if not is_string(query) then
        error('query must be string', 2)
    elseif params == nil then
        params = {}
    elseif not is_table(params) then
        error('params must be table', 2)
    end
    if deadline ~= nil and not is_uint(deadline) then
        error('deadline must be uint', 2)
    elseif single_row_mode ~= nil and not is_boolean(single_row_mode) then
        error('single_row_mode must be boolean', 2)
    end

    local err
    query, params, err = self:replace_named_params(query, params)
    if not query then
        return nil, err
    end

    local ok
    if #params == 0 then
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
--- @param deadline? integer
--- @return postgres.result? res
--- @return any err
--- @return boolean? timeout
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
--- @param conninfo? string
--- @param deadline integer
--- @return postgres.connection? conn
--- @return any err
--- @return boolean? timeout
local function new(conninfo, deadline)
    conninfo = conninfo or ''
    if not is_string(conninfo) then
        error('conninfo must be string', 2)
    elseif deadline ~= nil and not is_uint(deadline) then
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

    -- async connect
    local c = Connection(conn, conninfo)
    while true do
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
    end
end

return {
    new = new,
}
