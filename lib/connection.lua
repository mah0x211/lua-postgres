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
local sub = string.sub
local gsub = string.gsub
local concat = table.concat
local pcall = pcall
local type = type
local format = require('print').format
local errorf = require('error').format
local unpack = require('unpack')
local new_inet_client = require('net.stream.inet').client.new
local parse_conninfo = require('postgres.conninfo')
local new_cancel = require('postgres.cancel').new
local encode_message = require('postgres.message').encode
local decode_message = require('postgres.message').decode
--- constants
local INF_POS = math.huge
local INF_NEG = -math.huge

--- is_finite
--- @param v any
--- @return boolean ok
local function is_finite(v)
    return type(v) == 'number' and (v < INF_POS and v > INF_NEG)
end

--- noticefn
--- @param msg postgres.message.error_response
local function DEFAULT_NOTICEFN(msg)
    return print('[%s] %s', msg.severity, msg.message)
end

--- @class postgres.connection
--- @field private sock net.Socket
--- @field private conninfo string url encoded connection info string
--- @field private uri table<string, string> connection uri table
--- @field private noticefn fun(postgres.message.error_response)
--- @field private tracefn? fun(from:string, msg:string)
--- @field private parameter_statuses table<string, string>
--- @field private ready_for_query_status string
--- @field private backend_key_data postgres.message.backend_key_data
--- @field private error_response postgres.message.error_response
--- @field private buf string
--- @field private ready_for_query postgres.message.ready_for_query?
local Connection = {}

--- init
--- @param conninfo? string
--- @return postgres.connection?
--- @return any err
--- @return boolean? timeout
function Connection:init(conninfo)
    assert(conninfo == nil or type(conninfo) == 'string',
           'conninfo must be string or nil')

    -- parse connection info string
    local uri, err
    uri, err, conninfo = parse_conninfo(conninfo or '')
    if not uri then
        return nil, err
    end

    -- connect to server
    local host = uri.params.hostaddr or uri.host
    local sock, timeout
    sock, err, timeout = new_inet_client(host, uri.port, {
        deadline = uri.params.connect_timeout,
    })
    if not sock then
        return nil, err, timeout
    end

    self.sock = sock
    self.conninfo = conninfo
    self.uri = uri
    self.noticefn = DEFAULT_NOTICEFN
    self.parameter_statuses = {}
    self.backend_key_data = {}
    self.buf = ''

    -- send startup message
    local ok
    ok, err, timeout = self:startup()
    if not ok then
        sock:close()
        return nil, err, timeout
    end

    -- wait for ReadyForQuery message
    while true do
        local msg
        msg, err, timeout = self:recv()
        if not msg then
            self:close(true)
            return nil, err, timeout
        elseif msg.type == 'ErrorResponse' then
            self:close(true)
            return nil, errorf('[%s] %s', msg.severity, msg.message)
        elseif msg.type == 'BackendKeyData' then
            -- update backend_key_data
            self.backend_key_data = msg
        elseif msg.type == 'ReadyForQuery' then
            return self
        else
            self:close(true)
            return nil, errorf('BackendKeyData|ReadyForQuery expected, got %q',
                               msg.type)
        end
    end
end

--- startup
--- @private
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Connection:startup()
    -- startup message
    -- the possible responses are;
    --  * AuthenticationOk
    --  * AuthenticationCleartextPassword
    --  * AuthenticationMD5Password
    --  * AuthenticationSCMCredential
    --  * AuthenticationGSS
    --  * AuthenticationSSPI
    --  * AuthenticationGSSContinue
    --  * AuthenticationSASL
    --  * AuthenticationSASLContinue
    --  * AuthenticationSASLFinal
    --  * NegotiateProtocolVersion
    --  * ErrorResponse
    local len, err, timeout = self:send(encode_message.startup_message({
        user = self.uri.user,
        database = self.uri.dbname,
        application_name = self.uri.params.application_name,
        client_encoding = self.uri.params.client_encoding,
        datestyle = self.uri.params.datestyle,
        timezone = self.uri.params.timezone,
        geqo = self.uri.params.geqo,
    }))
    if not len then
        return false, err, timeout
    end

    -- authentication
    while true do
        local msg
        msg, err, timeout = self:recv()
        if not msg then
            return false, err, timeout
        end

        if msg.type == 'ErrorResponse' then
            self.error_response = msg
            return false, errorf('[%s] %s', msg.severity, msg.message)
        elseif msg.type ~= 'NegotiateProtocolVersion' then
            if msg.type == 'AuthenticationOk' then
                return true
            end

            if msg.type == 'AuthenticationCleartextPassword' then
                return self:authentication_cleartext_password()
            end
            return false,
                   errorf('unsuppported authentication type: %q', msg.type)
        end
    end
end

--- authentication_cleartext_password sends a cleartext password message
--- @private
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Connection:authentication_cleartext_password()
    -- password message
    -- the possible responses are;
    --  * AuthenticationOk
    --  * ErrorResponse
    local msg, err, timeout = self:password_message(self.uri.password)

    if not msg then
        return false, err, timeout
    elseif msg.type == 'ErrorResponse' then
        self.error_response = msg
        return false, errorf('[%s] %s', msg.severity, msg.message)
    elseif msg.type ~= 'AuthenticationOk' then
        return false, errorf('AuthenticationOk|ErrorResponse expected, got %q',
                             msg.type)
    end

    return true
end

--- password_message sends a postgres.message.password_message message
--- @private
--- @param pswd string
--- @return postgres.message? msg
--- @return any err
--- @return boolean? timeout
function Connection:password_message(pswd)
    assert(type(pswd) == 'string', 'pswd must be string')
    local len, err, timeout = self:send(encode_message.password_message(pswd))
    if not len then
        return nil, err, timeout
    end

    return self:recv()
end

--- send sends a message to the connection.
--- @param s string
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Connection:send(s)
    if not self.sock then
        return nil, errorf('connection is closed')
    end

    local len, err, timeout = self.sock:send(s)
    if not len then
        return false, err, timeout
    end
    self.ready_for_query = nil

    if self.tracefn then
        self.tracefn('client', s)
    end
    return true
end

--- recv retrieves a message from the connection.
--- if the following message types are received;
---   * ParameterStatus: update runtime parameters
---   * NoticeResponse: update notice field
---   * ReadyForQuery: update status and ready field
---
--- if a message type is other than the above message types
--- except ReadyForQuery, return the message
--- @return postgres.message? msg
--- @return any err
--- @return boolean? timeout
function Connection:recv()
    if not self.sock then
        return nil, errorf('connection is closed')
    end

    while not self.ready_for_query do
        local msg, err, again = decode_message(self.buf)
        if again then
            local s, timeout
            s, err, timeout = self.sock:recv()
            if not s then
                return nil, err, timeout
            end
            self.buf = self.buf .. s
        elseif not msg then
            return nil, err
        else
            -- consume bufferered data
            local data = sub(self.buf, 1, msg.consumed)
            self.buf = sub(self.buf, msg.consumed + 1)
            msg.consumed = nil

            if self.tracefn then
                self.tracefn('server', data)
            end

            if msg.type == 'ParameterStatus' then
                -- update parameter status
                self.parameter_statuses[msg.name] = msg.value
            elseif msg.type == 'NoticeResponse' then
                self.noticefn(msg)
            else
                -- print(dump {
                --     msg = msg,
                --     buf = self.buf,
                -- })
                if msg.type == 'ReadyForQuery' then
                    self.ready_for_query = msg
                end
                msg.conn = self
                return msg
            end
            -- continue to next message
        end
    end
end

--- close
--- @param force? boolean
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Connection:close(force)
    assert(force == nil or type(force) == 'boolean',
           'force must be boolean or nil')
    if not self.sock then
        return true
    end

    local len, err, timeout
    if not force then
        len, err, timeout = self:send(encode_message.terminate())
    end

    self.sock:close()
    self.sock = nil
    if not force and not len then
        -- failed to send terminate message
        return false, err, timeout
    end
    return true
end

--- is_connected
--- @return boolean connected
function Connection:is_connected()
    return self.sock ~= nil
end

--- get_conninfo
--- @return string conninfo
function Connection:get_conninfo()
    return self.conninfo
end

--- get_cancel
--- @return postgres.cancel cancel
--- @return any err
function Connection:get_cancel()
    return new_cancel(self.conninfo, self.backend_key_data.pid,
                      self.backend_key_data.key)
end

--- status
--- @return string?
---| 'idle'
---| 'transaction'
---| 'failed_transaction'
function Connection:status()
    if self.sock and self.ready_for_query then
        return self.ready_for_query.status
    end
end

--- parameter_status
--- @param param_name string
--- @return string? status
function Connection:parameter_status(param_name)
    return self.parameter_statuses[param_name]
end

--- server_version
--- @return string? version
function Connection:server_version()
    return self:parameter_status('server_version')
end

--- client_encoding
--- @return string encoding
function Connection:client_encoding()
    return self:parameter_status('client_encoding')
end

--- error_message
--- @return postgres.message.error_response? msg
function Connection:error_message()
    return self.error_response
end

--- backend_pid
--- @return integer pid
function Connection:backend_pid()
    return self.backend_key_data.pid
end

--- set_notice_receiver
--- @param noticefn function
function Connection:set_notice_receiver(noticefn)
    if noticefn == nil then
        noticefn = DEFAULT_NOTICEFN
    elseif type(noticefn) ~= 'function' then
        error('noticefn must be function')
    end
    self.noticefn = noticefn
end

--- trace
--- @param tracefn? fun(from:string, msg:string)
--- @return function oldfn
function Connection:trace(tracefn)
    assert(tracefn == nil or type(tracefn) == 'function',
           'tracefn must be function or nil')
    local oldfn = self.tracefn
    self.tracefn = tracefn
    return oldfn
end

--- flush sends a postgres.message.flush message
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Connection:flush()
    local len, err, timeout = self:send(encode_message.flush())
    if not len then
        return false, err, timeout
    end
    return true
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
--- @param params table<string, any>
--- @return string? query
--- @return any err
--- @return table? params
function Connection:replace_named_params(query, params)
    assert(type(query) == 'string', 'query must be string')
    assert(type(params) == 'table', 'params must be table')

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
--- @param params table<string, any>?
--- @param max_rows integer?
--- @return postgres.message? msg
--- @return any err
--- @return boolean? timeout
function Connection:query(query, params, max_rows)
    assert(type(query) == 'string', 'query must be string')
    assert(params == nil or type(params) == 'table',
           'params must be table or nil')
    assert(max_rows == nil or is_finite(max_rows),
           'max_rows must be integer or nil')
    if not self.ready_for_query then
        return nil, errorf('connection is not ready for query')
    end

    if params == nil then
        params = {}
    end

    if max_rows == nil then
        max_rows = 0
    end

    local parsed_query, err, values = self:replace_named_params(query, params)
    if not parsed_query then
        return nil, err
    end

    if #values == 0 and max_rows == 0 then
        return self:simple_query(parsed_query)
    end
    return self:extended_query(parsed_query, values, max_rows)
end

--- simple_query
--- @private
--- @param query string
--- @return postgres.message? msg
--- @return any err
--- @return boolean? timeout
function Connection:simple_query(query)
    -- The possible response messages from the backend are:
    --  * CommandComplete
    --  * CopyInResponse
    --  * CopyOutResponse
    --  * RowDescription
    --  * DataRow
    --  * EmptyQueryResponse
    --  * ErrorResponse
    --  * NoticeResponse
    --  * ReadyForQuery
    local len, err, timeout = self:send(encode_message.query(query))
    if not len then
        return nil, err, timeout
    end
    return self:next()
end

--- extended_query
--- @private
--- @param query string
--- @param values string[]
--- @param max_rows integer?
--- @return postgres.message? res
--- @return any err
--- @return boolean? timeout
function Connection:extended_query(query, values, max_rows)
    local len, err, timeout = self:send(concat({
        -- prepare query
        -- the possible responses are:
        --  * ParseComplete
        --  * ErrorResponse
        encode_message.parse('', query), -- unnamed statement

        -- bind parameters to the prepared query
        -- the possible responses are:
        --  * BindComplete
        --  * ErrorResponse
        encode_message.bind('', '', values), -- unnamed portal and statement

        -- describe portal
        -- the possible responses are:
        --  * RowDescription
        --  * NoData
        --  * ErrorResponse
        encode_message.describe_portal(''), -- unnamed portal

        -- execute portal
        -- the possible responses are:
        --  * CommandComplete
        --  * CopyInResponse
        --  * CopyOutResponse
        --  * DataRow
        --  * EmptyQueryResponse
        --  * ErrorResponse
        --  * NoticeResponse
        encode_message.execute(''), -- unnamed portal

        -- close_statement
        -- the possible responses are:
        --  * CloseComplete
        --  * ErrorResponse
        encode_message.close_statement(''), -- unnamed statement

        -- sync
        -- the possible responses are:
        --  * ReadyForQuery
        --  * ErrorResponse
        encode_message.sync(),
    }))
    if not len then
        return nil, err, timeout
    end

    -- wait for ParseComplete and BindComplete messages
    local target = 'ParseComplete'
    local msg
    while true do
        msg, err, timeout = self:recv()
        if not msg then
            return nil, err, timeout
        elseif msg.type == 'ErrorResponse' then
            self.error_response = msg
            return msg
        elseif msg.type ~= target then
            return nil, errorf(
                       target .. '|ErrorResponse expects, got %q response',
                       msg.type)
        elseif target == 'ParseComplete' then
            target = 'BindComplete'
        elseif target == 'BindComplete' then
            break
        end
    end

    -- wait for RowDescription or NoData message
    msg, err, timeout = self:recv()
    if not msg then
        return nil, err, timeout
    elseif msg.type == 'ErrorResponse' then
        self.error_response = msg
        -- return error message
        return msg
    elseif msg.type == 'RowDescription' then
        return msg
    elseif msg.type == 'NoData' then
        return self:next()
    else
        return nil, errorf(
                   'RowDescription|NoData|ErrorResponse expects, got %q response',
                   msg.type)
    end
end

--- next retrieves a next message from the connection.
--- if you sent a query message, you must retrieve a response message from the
--- server until it returns a ReadyForQuery message.
--- this method will return the message except the following message types:
---  * DataRow
---  * CloseComplete
---  * NoticeResponse
--- @return postgres.message? msg
--- @return any err
--- @return boolean? timeout
function Connection:next()
    --
    -- the possible responses are:
    --  * CommandComplete
    --  * CopyInResponse
    --  * CopyOutResponse
    --  * RowDescription
    --  * DataRow
    --  * EmptyQueryResponse
    --  * ErrorResponse
    --  * ReadyForQuery
    --  * NoticeResponse
    --
    -- it returns result if the message type is not the following:
    --  * DataRow
    --  * CloseComplete
    --  * NoticeResponse
    --
    while true do
        local msg, err, timeout = self:recv()
        if not msg then
            return nil, err, timeout
        elseif msg.type == 'ReadyForQuery' then
            return msg
        elseif msg.type == 'ErrorResponse' then
            self.error_response = msg
            return msg
        elseif msg.type ~= 'DataRow' and msg.type ~= 'CloseComplete' then
            return msg
        end
        -- ignore DataRow and CloseComplete messages
    end
end

return {
    new = require('metamodule').new(Connection),
}
