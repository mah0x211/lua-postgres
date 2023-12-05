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
local concat = table.concat
local errorf = require('error').format
local unpack = require('postgres.unpack')
local htonl = require('postgres.htonl')
--- constants
local NULL = '\0'

--- @class postgres.message.authentication : postgres.message
--- @field salt string? AuthenticationMD5Password
--- @field data string? AuthenticationGSSContinue | AuthenticationSASLContinue AuthenticationSASLFinal
--- @field mechanisms string[]? AuthenticationSASL.mechanisms
local Authentication = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return postgres.message.authentication? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    if #s < 1 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'R' then
        return nil, errorf('invalid Authentication message')
    end

    --
    -- Authentication* Message Header
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32
    --     Specifies that the authentication status code.
    --
    local header = {}
    local _, err, again = unpack(header, 'b1Li', s)
    if err then
        return nil, errorf('invalid Authentication message', err)
    elseif again then
        return nil, nil, true
    end

    local len = header[2] + 1
    local code = header[3]
    local msg = Authentication()
    --
    -- AuthenticationOk (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(0)
    --     Specifies that the authentication was successful.
    --
    if code == 0 then
        msg.consumed = len
        msg.type = 'AuthenticationOk'
        return msg
    end

    --
    -- AuthenticationKerberosV5 (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(2)
    --     Specifies that Kerberos V5 authentication is required.
    --
    if code == 2 then
        msg.consumed = len
        msg.type = 'AuthenticationKerberosV5'
        return msg
    end

    --
    -- AuthenticationCleartextPassword (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(3)
    --     Specifies that a clear-text password is required.
    --
    if code == 3 then
        msg.consumed = len
        msg.type = 'AuthenticationCleartextPassword'
        return msg
    end

    --
    -- AuthenticationMD5Password (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(12)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(5)
    --     Specifies that an MD5-encrypted password is required.
    --
    --   Byte4
    --     The salt to use when encrypting the password.
    --
    if code == 5 then
        msg.consumed = len
        msg.type = 'AuthenticationMD5Password'
        msg.salt = sub(s, 10, 13)
        return msg
    end

    --
    -- AuthenticationSCMCredential (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(6)
    --     Specifies that an SCM credentials message is required.
    --
    if code == 6 then
        msg.consumed = len
        msg.type = 'AuthenticationSCMCredential'
        return msg
    end

    --
    -- AuthenticationGSS (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(7)
    --     Specifies that GSSAPI authentication is required.
    --
    if code == 7 then
        msg.consumed = len
        msg.type = 'AuthenticationGSS'
        return msg
    end

    --
    -- AuthenticationGSSContinue (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32(8)
    --     Specifies that this message contains GSSAPI or SSPI data.
    --
    --   Byten
    --     GSSAPI or SSPI authentication data.
    --
    if code == 8 then
        msg.consumed = len
        msg.type = 'AuthenticationGSSContinue'
        msg.data = sub(s, 10, len)
        return msg
    end

    --
    -- AuthenticationSSPI (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(9)
    --     Specifies that SSPI authentication is required.
    --
    if code == 9 then
        msg.consumed = len
        msg.type = 'AuthenticationSSPI'
        return msg
    end

    --
    -- AuthenticationSASL (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32(10)
    --     Specifies that SASL authentication is required.
    --
    -- The message body is a list of SASL authentication mechanisms, in the
    -- server's order of preference. A zero byte is required as terminator after
    -- the last authentication mechanism name. For each mechanism, there is the
    -- following:
    --
    --   String
    --     Name of a SASL authentication mechanism.
    --
    if code == 10 then
        s = sub(s, 10, len)
        local mechanisms = {}
        local tail = find(s, NULL, 1, true)
        while tail do
            if tail == 1 then
                -- found terminator for the last authentication mechanism name
                s = sub(s, 2)
                break
            end
            mechanisms[#mechanisms + 1] = sub(s, 1, tail - 1)
            s = sub(s, tail + 1)
            tail = find(s, NULL, 1, true)
        end
        if #s > 0 then
            return nil, errorf(
                       'invalid AuthenticationSASL message: message length is too long (unknown %d bytes of data remains)',
                       #s)
        end

        msg.consumed = len
        msg.type = 'AuthenticationSASL'
        msg.mechanisms = mechanisms
        return msg
    end

    --
    -- AuthenticationSASLContinue (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32(11)
    --     Specifies that this message contains a SASL challenge.
    --
    --   Byten
    --     SASL data, specific to the SASL mechanism being used.
    --
    if code == 11 then
        msg.consumed = len
        msg.type = 'AuthenticationSASLContinue'
        msg.data = sub(s, 10, len)
        return msg
    end

    --
    -- AuthenticationSASLFinal (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32(12)
    --     Specifies that SASL authentication has completed.
    --
    --   Byten
    --     SASL outcome "additional data", specific to the SASL mechanism being
    --     used.
    --
    if code == 12 then
        msg.consumed = len
        msg.type = 'AuthenticationSASLFinal'
        msg.data = sub(s, 10, len)
        return msg
    end

    return nil, errorf('unsupported Authentication message')
end

--- encode
--- @param auth string
---| '"AuthenticationOk"' # AuthenticationOk (B)
---| '"AuthenticationCleartextPassword"' # AuthenticationCleartextPassword (B)
---| '"AuthenticationMD5Password"' # AuthenticationMD5Password (B)
---| '"AuthenticationSCMCredential"' # AuthenticationSCMCredential (B)
---| '"AuthenticationGSS"' # AuthenticationGSS (B)
---| '"AuthenticationGSSContinue"' # AuthenticationGSSContinue (B)
---| '"AuthenticationSSPI"' # AuthenticationSSPI (B)
---| '"AuthenticationSASL"' # AuthenticationSASL (B)
---| '"AuthenticationSASLContinue"' # AuthenticationSASLContinue (B)
---| '"AuthenticationSASLFinal"' # AuthenticationSASLFinal (B)
--- @param ... string
--- @return string
local function encode(auth, ...)
    assert(type(auth) == 'string', 'portal must be string')

    --
    -- AuthenticationOk (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(0)
    --     Specifies that the authentication was successful.
    --
    if auth == 'AuthenticationOk' then
        return 'R' .. htonl(8) .. htonl(0)
    end

    --
    -- AuthenticationKerberosV5 (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(2)
    --     Specifies that Kerberos V5 authentication is required.
    --
    if auth == 'AuthenticationKerberosV5' then
        return 'R' .. htonl(8) .. htonl(2)
    end

    --
    -- AuthenticationCleartextPassword (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(3)
    --     Specifies that a clear-text password is required.
    --
    if auth == 'AuthenticationCleartextPassword' then
        return 'R' .. htonl(8) .. htonl(3)
    end

    --
    -- AuthenticationMD5Password (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(12)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(5)
    --     Specifies that an MD5-encrypted password is required.
    --
    --   Byte4
    --     The salt to use when encrypting the password.
    --
    if auth == 'AuthenticationMD5Password' then
        local salt = ...
        assert(type(salt) == 'string' and #salt == 4,
               'argument#2 salt must be 4 bytes string')
        return 'R' .. htonl(12) .. htonl(5) .. salt
    end

    --
    -- AuthenticationSCMCredential (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(6)
    --     Specifies that an SCM credentials message is required.
    --
    if auth == 'AuthenticationSCMCredential' then
        return 'R' .. htonl(8) .. htonl(6)
    end

    --
    -- AuthenticationGSS (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(7)
    --     Specifies that GSSAPI authentication is required.
    --
    if auth == 'AuthenticationGSS' then
        return 'R' .. htonl(8) .. htonl(7)
    end

    --
    -- AuthenticationGSSContinue (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32(8)
    --     Specifies that this message contains GSSAPI or SSPI data.
    --
    --   Byten
    --     GSSAPI or SSPI authentication data.
    --
    if auth == 'AuthenticationGSSContinue' then
        local data = ...
        assert(type(data) == 'string',
               'argument#2 GSSAPI or SSPI data must be string')
        return 'R' .. htonl(#data + 8) .. htonl(8) .. data
    end

    --
    -- AuthenticationSSPI (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32(8)
    --     Length of message contents in bytes, including self.
    --
    --   Int32(9)
    --     Specifies that SSPI authentication is required.
    --
    if auth == 'AuthenticationSSPI' then
        return 'R' .. htonl(8) .. htonl(9)
    end

    --
    -- AuthenticationSASL (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32(10)
    --     Specifies that SASL authentication is required.
    --
    -- The message body is a list of SASL authentication mechanisms, in the
    -- server's order of preference. A zero byte is required as terminator after
    -- the last authentication mechanism name. For each mechanism, there is the
    -- following:
    --
    --   String
    --     Name of a SASL authentication mechanism.
    --
    if auth == 'AuthenticationSASL' then
        local n = select('#', ...)
        assert(n > 0, 'argument#2 SASL authentication mechanism name required')

        local list = {}
        for i = 1, n do
            local name = select(i, ...)
            assert(type(name) == 'string',
                   'argument#' .. i + 2 ..
                       ' SASL authentication mechanism name ' ..
                       'must be string')
            list[i] = name
        end
        local mechanisms = concat(list, NULL) .. NULL .. NULL
        return 'R' .. htonl(8 + #mechanisms) .. htonl(10) .. mechanisms
    end

    --
    -- AuthenticationSASLContinue (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32(11)
    --     Specifies that this message contains a SASL challenge.
    --
    --   Byten
    --     SASL data, specific to the SASL mechanism being used.
    --
    if auth == 'AuthenticationSASLContinue' then
        local data = ...
        assert(type(data) == 'string', 'argument#2 SASL data must be string')
        return 'R' .. htonl(#data + 8) .. htonl(11) .. data
    end

    --
    -- AuthenticationSASLFinal (B)
    --   Byte1('R')
    --     Identifies the message as an authentication request.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32(12)
    --     Specifies that SASL authentication has completed.
    --
    --   Byten
    --     SASL outcome "additional data", specific to the SASL mechanism being
    --     used.
    --
    if auth == 'AuthenticationSASLFinal' then
        local data = ...
        assert(type(data) == 'string',
               'argument#2 SASL outcome "additional data" must be string')
        return 'R' .. htonl(#data + 8) .. htonl(12) .. data
    end

    error(errorf('unsupported Authentication message: %q', auth))
end

return {
    encode = encode,
    decode = decode,
}
