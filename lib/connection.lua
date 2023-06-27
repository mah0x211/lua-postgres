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
local wait = require('io.wait')
local io_readable = wait.readable
local io_writable = wait.writable
local poll = require('gpoll')
local pollable = poll.pollable
local poll_unwait = poll.unwait
local poll_readable = poll.readable
local poll_writable = poll.writable

--- @type fun(conn: postgres.connection, res: postgres.pgresult):postgres.result
local new_result = require('postgres.result').new

--- define postgres.pgresult metatable
--- @class postgres.pgresult
--- @field clear fun(self: postgres.pgresult)
--- @field connection fun(self: postgres.pgresult):postgres.pgconn
--- @field status fun(self: postgres.pgresult):string
--- @field error_message fun(self: postgres.pgresult):string?
--- @field verbose_error_message fun(self: postgres.pgresult, verbosity?:string, visibility?:string):(errmsg:string?, err:any)
--- @field error_field fun(self: postgres.pgresult, field:string):(value:string?, err:any)
--- @field ntuples fun(self: postgres.pgresult):integer
--- @field nfields fun(self: postgres.pgresult):integer
--- @field binary_tuples fun(self: postgres.pgresult):boolean
--- @field fname fun(self: postgres.pgresult, column:integer):string?
--- @field fnumber fun(self: postgres.pgresult, column:string):integer?
--- @field ftable fun(self: postgres.pgresult, column:integer):integer?
--- @field ftablecol fun(self: postgres.pgresult, column:integer):integer?
--- @field fformat fun(self: postgres.pgresult, column:integer):string
--- @field ftype fun(self: postgres.pgresult, column:integer):integer
--- @field fsize fun(self: postgres.pgresult, column:integer):integer
--- @field fmod fun(self: postgres.pgresult, column:integer):integer
--- @field cmd_status fun(self: postgres.pgresult):string
--- @field oid_value fun(self: postgres.pgresult):integer
--- @field cmd_tuples fun(self: postgres.pgresult):(ntuples:integer?, err:any)
--- @field get_value fun(self: postgres.pgresult, row:integer, column:integer):string
--- @field get_length fun(self: postgres.pgresult, row:integer, column:integer):integer
--- @field get_is_null fun(self: postgres.pgresult, row:integer, column:integer):boolean
--- @field nparams fun(self: postgres.pgresult):integer
--- @field param_type fun(self: postgres.pgresult, param_num:integer):integer

--- define postres.pgcancel metatable
--- @class postgres.pgcancel
--- @field free fun(self: postgres.pgcancel):boolean
--- @field cancel fun(self: postgres.pgcancel):(ok:boolean, err:any)

--- define postgres.pgconn metatable
--- @class postgres.pgconn
--- @field finish fun(self: postgres.pgconn)
--- @field conninfo fun(self: postgres.pgconn):(info:table?, err:any)
--- @field connect_poll fun(self: postgres.pgconn):string
--- @field get_cancel fun(self: postgres.pgconn):(canceler:postgres.pgcancel?, err:any)
--- @field db fun(self: postgres.pgconn):string
--- @field user fun(self: postgres.pgconn):string
--- @field pass fun(self: postgres.pgconn):string
--- @field host fun(self: postgres.pgconn):string
--- @field hostaddr fun(self: postgres.pgconn):string
--- @field port fun(self: postgres.pgconn):string
--- @field options fun(self: postgres.pgconn):string
--- @field status fun(self: postgres.pgconn):string
--- @field transaction_status fun(self: postgres.pgconn):string
--- @field parameter_status fun(self: postgres.pgconn, name:string):string
--- @field protocol_version fun(self: postgres.pgconn):integer
--- @field server_version fun(self: postgres.pgconn):integer
--- @field error_message fun(self: postgres.pgconn):string
--- @field socket fun(self: postgres.pgconn):integer
--- @field backend_pid fun(self: postgres.pgconn):integer
--- @field pipeline_status fun(self: postgres.pgconn):string
--- @field connection_needs_password fun(self: postgres.pgconn):boolean
--- @field connection_used_password fun(self: postgres.pgconn):boolean
--- @field client_encoding fun(self: postgres.pgconn):string
--- @field set_client_encoding fun(self: postgres.pgconn, encoding:string):(ok:boolean, err:any)
--- @field ssl_in_use fun(self: postgres.pgconn):boolean
--- @field ssl_attribute fun(self: postgres.pgconn, name:string):string
--- @field ssl_attribute_names fun(self: postgres.pgconn):table
--- @field set_error_verbosity fun(self: postgres.pgconn, verbosity?:string):(old:string)
--- @field set_error_context_visibility fun(self: postgres.pgconn, context?:string):(old:string)
--- @field set_notice_processor fun(self: postgres.pgconn, fn:function, ...):(old:string)
--- @field set_notice_receiver fun(self: postgres.pgconn, fn:function, ...):(old:string)
--- @field call_notice_processor fun(self: postgres.pgconn, msg:string):boolean
--- @field call_notice_receiver fun(self: postgres.pgconn, res:postgres.pgresult):boolean
--- @field trace fun(self: postgres.pgconn, stream:file*):(old:file*)
--- @field untrace fun(self: postgres.pgconn):(old:file*)
--- @field set_trace_flags fun(self: postgres.pgconn, flg:string, ...)
--- @field exec fun(self: postgres.pgconn, command:string):(result:postgres.pgresult?, err:any)
--- @field exec_params fun(self: postgres.pgconn, command:string, ...):(result:postgres.pgresult?, err:any)
--- @field send_query fun(self: postgres.pgconn, query:string):(ok:boolean, err:any)
--- @field send_query_params fun(self: postgres.pgconn, query:string, ...):(ok:boolean, err:any)
--- @field set_single_row_mode fun(self: postgres.pgconn):boolean
--- @field get_result fun(self: postgres.pgconn):(result:postgres.pgresult?, err:any)
--- @field is_busy fun(self: postgres.pgconn):(busy:boolean, err:any)
--- @field consume_input fun(self: postgres.pgconn):(ok:boolean, err:any)
--- @field enter_pipeline_mode fun(self: postgres.pgconn):(ok:boolean, err:any)
--- @field exit_pipeline_mode fun(self: postgres.pgconn):(ok:boolean, err:any)
--- @field pipeline_sync fun(self: postgres.pgconn):(ok:boolean, err:any)
--- @field send_flush_request fun(self: postgres.pgconn):(ok:boolean, err:any)
--- @field notifies fun(self: postgres.pgconn):(data:table?, err:any)
--- @field put_copy_data fun(self: postgres.pgconn, data:string):(ok:boolean, err:any, again:boolean?)
--- @field put_copy_end fun(self: postgres.pgconn, errmsg?:string):(ok:boolean, err:any, again:boolean?)
--- @field get_copy_data fun(self: postgres.pgconn, async?:boolean):(data:string?, err:any, again:boolean?)
--- @field set_nonblocking fun(self: postgres.pgconn, enable:boolean):(ok:boolean, err:any)
--- @field is_nonblocking fun(self: postgres.pgconn):boolean
--- @field flush fun(self: postgres.pgconn):(ok:boolean, err:any, again:boolean?)
--- @field make_empty_result fun(self: postgres.pgconn, status?:string):(postgres.pgresult, err:any)
--- @field escape_string_conn fun(self: postgres.pgconn, str:string):(escaped:string?, err:any)
--- @field escape_literal fun(self: postgres.pgconn, str:string):(escaped:string?, err:any)
--- @field escape_identifier fun(self: postgres.pgconn, str:string):(escaped:string?, err:any)
--- @field escape_bytea_conn fun(self: postgres.pgconn, str:string):(escaped:string?, err:any)
--- @field encrypt_password_conn fun(self: postgres.pgconn, user:string, password:string, algorithm?:string):(encrypted:string?, err:any)

--- @type fun(conninfo?: string, nonblock?: boolean): postgres.pgconn
local pgconn = require('postgres.pgconn')

local DEFAULT_DEADLINE = 3000

--- @class postgres.connection : postgres
--- @field conn postgres.pgconn
--- @field nonblock boolean
local Connection = {}

--- init
--- @param conn postgres.pgconn
--- @param conninfo string
--- @return postgres
function Connection:init(conn, conninfo)
    self.conn = conn
    self.conninfo = conninfo
    return self
end

--- wait_readable
--- @param msec? integer
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Connection:wait_readable(msec)
    local wait_readable = pollable() and poll_readable or io_readable
    -- wait until readable
    return wait_readable(self.conn:socket(), msec or DEFAULT_DEADLINE)
end

--- wait_writable
--- @param msec? integer
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Connection:wait_writable(msec)
    local wait_writable = pollable() and poll_writable or io_writable
    -- wait until writable
    return wait_writable(self.conn:socket(), msec or DEFAULT_DEADLINE)
end

--- close
function Connection:close()
    if self.nonblock then
        poll_unwait(self.conn:socket())
    end
    self.conn:finish()
end

--- get_cancel
--- @return postgres.pgcancel? cancel
--- @return any err
function Connection:get_cancel()
    return self.conn:get_cancel()
end

--- status
--- @return string status
function Connection:status()
    return self.conn:status()
end

--- transaction_status
--- @return string status
function Connection:transaction_status()
    return self.conn:transaction_status()
end

--- parameter_status
--- @param param_name string
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
--- @return string status
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
function Connection:ssl_in_use()
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
--- @param verbosity string
--- @return string verbosity
function Connection:set_error_verbosity(verbosity)
    return self.conn:set_error_verbosity(verbosity)
end

--- set_error_context_visibility
--- @param visibility string
--- @return string visibility
function Connection:set_error_context_visibility(visibility)
    return self.conn:set_error_context_visibility(visibility)
end

--- set_notice_processor
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
--- @param res postgres.result
--- @return boolean ok
function Connection:call_notice_receiver(res)
    return self.conn:call_notice_receiver(res.res)
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
--- @params ... string
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
--- @param msec? integer
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Connection:flush(msec)
    while true do
        local ok, err, again = self.conn:flush()
        if not again then
            return ok, err
        end

        local timeout
        ok, err, timeout = self:wait_writable(msec)
        if not ok then
            return false, err, timeout
        end
    end
end

--- stringify
--- @param val any
--- @return any val
--- @return string type
local function stringify(val)
    local typ = type(val)
    if typ == 'string' then
        return val, typ
    elseif typ == 'nil' then
        return 'NULL', typ
    elseif typ == 'boolean' then
        return val and 'TRUE' or 'FALSE', typ
    elseif typ == 'number' then
        return tostring(val), typ
    elseif typ == 'table' then
        return val, typ
    end
    return nil, typ
end

--- replace_named_params
--- @param query string
--- @param params table
--- @return string? query
--- @return any err
--- @return table? params
function Connection:replace_named_params(query, params)
    assert(is_string(query), 'query must be string')
    assert(is_table(params), 'params must be table')

    local newparams = {
        unpack(params),
    }
    local param_ids = {}
    local ok, res = pcall(gsub, query, '%${([^}]+)}', function(name)
        if param_ids[name] then
            return param_ids[name]
        end

        -- convert to positional parameters
        local val, typ = stringify(params[name])

        if not val then
            error(format('invalid parameter %q: data type %q is not supported',
                         name, typ))
        elseif typ ~= 'table' then
            -- add positional parameter
            newparams[#newparams + 1] = val
            param_ids[name] = '$' .. #newparams
            return param_ids[name]
        end

        -- convert table to array
        local stack = {}
        local ctx = {
            ids = {},
            tbl = val,
        }
        ctx.idx, val = next(ctx.tbl)
        while val do
            val, typ = stringify(val)
            if not val then
                error(format(
                          'invalid parameter %q: data type %q is not supported',
                          name, typ))
            elseif typ ~= 'table' then
                -- convert to positional parameters
                newparams[#newparams + 1] = val
                ctx.ids[#ctx.ids + 1] = '$' .. #newparams
            else
                stack[#stack + 1] = ctx
                ctx = {
                    ids = {},
                    tbl = val,
                }
            end

            ctx.idx, val = next(ctx.tbl, ctx.idx)
            if not val then
                while #stack > 0 do
                    local child = ctx
                    ctx, stack[#stack] = stack[#stack], nil
                    ctx.ids[#ctx.ids + 1] =
                        '{' .. concat(child.ids, ', ') .. '}'
                    ctx.idx, val = next(ctx.tbl, ctx.idx)
                    if val then
                        break
                    end
                end
            end
        end

        param_ids[name] = concat(ctx.ids, ', ')
        return param_ids[name]
    end)

    if not ok then
        return nil, res
    end

    return res, nil, newparams
end

--- query
--- @param query string
--- @param params? table?
--- @param msec? integer
--- @param single_row_mode? boolean
--- @return postgres.result? res
--- @return any err
--- @return boolean? timeout
function Connection:query(query, params, msec, single_row_mode)
    assert(is_string(query), 'query must be string')
    assert(params == nil or is_table(params), 'params must be table or nil')
    assert(msec == nil or is_uint(msec), 'msec must be uint or nil')
    assert(single_row_mode == nil or is_boolean(single_row_mode),
           'single_row_mode must be boolean or nil')
    if params == nil then
        params = {}
    end

    local err
    query, err, params = self:replace_named_params(query, params)
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
    ok, err, timeout = self:flush(msec)
    if not ok then
        return nil, err, timeout
    elseif single_row_mode then
        assert(self.conn:set_single_row_mode(), 'failed to set single row mode')
    end

    return self:get_result(msec)
end

--- get_result
--- @param msec? integer
--- @return postgres.result? res
--- @return any err
--- @return boolean? timeout
function Connection:get_result(msec)
    assert(msec == nil or is_uint(msec), 'msec must be uint or nil')

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
        local ok, werr, timeout = self:wait_readable(msec)
        if not ok then
            return nil, werr, timeout
        end
    end
end

--- make_empty_result
--- @param status string
--- @return postgres.result? res
--- @return any err
function Connection:make_empty_result(status)
    local res, err = self.conn:make_empty_result(status)
    if not res then
        return nil, err
    end
    return new_result(self, res)
end

Connection = require('metamodule').new(Connection)

--- connect
--- @param conninfo? string
--- @param msec? integer
--- @return postgres.connection? conn
--- @return any err
--- @return boolean? timeout
local function new(conninfo, msec)
    assert(conninfo == nil or is_string(conninfo),
           'conninfo must be string or nil')
    assert(msec == nil or is_uint(msec), 'msec must be uint or nil')

    local is_pollable = pollable()
    local is_nonblock = is_pollable or msec ~= nil
    local conn, err = pgconn(conninfo, is_nonblock)
    if err then
        return nil, err
    end

    -- check status
    local status = conn:status()
    if status == 'bad' then
        return nil, conn:error_message()
    end

    -- async connect
    local c = Connection(conn, conninfo or '')
    while true do
        -- check status
        status = conn:connect_poll()
        if status == 'ok' then
            return c
        elseif status == 'failed' then
            return nil, conn:error_message()
        end

        -- polling a status
        local ok, timeout
        if status == 'reading' then
            ok, err, timeout = c:wait_readable(msec)
        elseif status == 'writing' then
            ok, err, timeout = c:wait_writable(msec)
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
