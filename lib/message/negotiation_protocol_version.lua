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
local find = string.find
local sub = string.sub
local errorf = require('error').format
local ntohl = require('postgres.ntohl')
--- constants
local NULL = '\0'

--- @class postgres.message.negotiation_protocol_version : postgres.message
--- @field minor_version integer
--- @field options string[]
local NegotiateProtocolVersion = require('metamodule').new({},
                                                           'postgres.message')

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- NegotiateProtocolVersion (B)
    --   Byte1('v')
    --     Identifies the message as a protocol version negotiation message.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32
    --     Newest minor protocol version supported by the server for the major protocol version requested by the client.
    --
    --   Int32
    --     Number of protocol options not recognized by the server.
    --
    -- Then, for protocol option not recognized by the server, there is the following:
    --
    --   String
    --     The option name.
    --
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'v' then
        return nil, errorf('invalid NegotiateProtocolVersion message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if #s < consumed then
        return nil, nil, true
    end

    local minor_version = ntohl(sub(s, 6))
    local noption = ntohl(sub(s, 10))
    local options = {}
    local head = 14
    for i = 1, noption do
        -- null terminated string
        local tail = find(s, NULL, head, true)
        if not tail or tail > consumed then
            return nil, errorf('invalid NegotiateProtocolVersion message')
        end
        options[i] = sub(s, head, tail - 1)
        head = tail + 1
    end
    if head - consumed ~= 1 then
        -- message is not consumed correctly
        return nil, errorf('invalid NegotiateProtocolVersion message')
    end

    local msg = NegotiateProtocolVersion()
    msg.consumed = consumed
    msg.type = 'NegotiateProtocolVersion'
    msg.minor_version = minor_version
    msg.options = options
    return msg
end

return {
    decode = decode,
}
