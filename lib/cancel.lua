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
local type = type
local errorf = require('error').format
local new_inet_client = require('net.stream.inet').client.new
local parse_conninfo = require('postgres.conninfo')
local encode_cancel_request = require('postgres.message.cancel_request').encode
local decode_message = require('postgres.message').decode

--- @class postgres.cancel
--- @field private msg string cancel message
--- @field conninfo string
--- @field params table
--- @field pid integer process ID of the target backend
--- @field key integer secret key for the target backend
local Cancel = {}

--- init
--- @param conninfo string
--- @param pid integer process ID of the target backend
--- @param key integer secret key for the target backend
--- @return postgres.cancel
--- @return any err
function Cancel:init(conninfo, pid, key)
    assert(type(conninfo) == 'string', 'conninfo must be string')
    assert(type(pid) == 'number', 'pid must be integer')
    assert(type(key) == 'number', 'key must be integer')

    -- parse conninfo
    local uri, err
    uri, err, conninfo = parse_conninfo(conninfo)
    if not uri then
        return nil, err
    end
    self.conninfo = conninfo
    self.uri = uri
    self.pid = pid
    self.key = key
    self.msg = encode_cancel_request(pid, key)
    return self
end

--- cancel
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Cancel:cancel()
    -- connect to server
    local host = self.uri.params.hostaddr or self.uri.host
    local sock, err, timeout = new_inet_client(host, self.uri.port, {
        deadline = self.uri.params.connect_timeout,
    })
    if not sock then
        return false, err, timeout
    end

    -- send cancel message
    local len
    len, err, timeout = sock:send(self.msg)
    if not len then
        sock:close()
        return false, err, timeout
    end

    -- server may disconnect immediately after receiving cancel message
    local buf = ''
    while true do
        local s
        s, err, timeout = sock:recv()
        if not s then
            sock:close()
            if err then
                return false, err, timeout
            end
            return true
        end
        buf = buf .. s

        -- decode message
        local msg
        msg, err = decode_message(buf)
        if msg then
            sock:close()
            return false, errorf('unexpected message %q received after cancel',
                                 msg.type)
        elseif err then
            sock:close()
            return false, err
        end
    end
end

return {
    new = require('metamodule').new(Cancel),
}

