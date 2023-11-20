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
local sub = string.sub
local errorf = require('error').format
local ntohl = require('postgres.ntohl')

--- @class postgres.message.authentication : postgres.message
--- @field salt string? AuthenticationMD5Password
--- @field data string? AuthenticationGSSContinue | AuthenticationSASLContinue AuthenticationSASLFinal
--- @field name string? AuthenticationSASL.name
local Authentication = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'R' then
        return nil, errorf('invalid Authentication message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if #s < consumed then
        return nil, nil, true
    end
    local code = ntohl(sub(s, 6))

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
        msg.consumed = consumed
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
        msg.consumed = consumed
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
        msg.consumed = consumed
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
        msg.consumed = consumed
        msg.type = 'AuthenticationMD5Password'
        msg.salt = sub(s, 10, 13)
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
    if code == 6 then
        msg.consumed = consumed
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
        msg.consumed = consumed
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
        msg.consumed = consumed
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
        msg.consumed = consumed
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
        msg.consumed = consumed
        msg.type = 'AuthenticationSASL'
        msg.name = sub(s, 10, len)
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
        msg.consumed = consumed
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
        msg.consumed = consumed
        msg.type = 'AuthenticationSASLFinal'
        msg.data = sub(s, 10, len)
        return msg
    end

    return nil, errorf('unknown Authentication message')
end

return {
    decode = decode,
}
