--
-- Copyright (C) 2025 Masatoshi Fukunaga
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
local htonl = require('postgres.htonl')
local ntohl = require('postgres.ntohl')
local errorf = require('error').format

--
-- CopyDone (F/B)
--   Byte1('c')
--     Identifies the message as a CopyDone.
--
--   Int32 (4 bytes)
--     Length of message contents in bytes, including self.
--
--   This message is sent from a frontend during a COPY FROM operation to
--   signal successful completion of the copy operation, and from a backend
--   during a COPY TO operation to indicate the end of the data stream.
--

--- @class postgres.message.copy_done : postgres.message
local CopyDone = require('metamodule').new({}, 'postgres.message')

--- encode
--- @return string msg
local function encode()
    return 'c' .. htonl(4)
end

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'c' then
        return nil, errorf('invalid CopyDone message')
    end

    local len = ntohl(sub(s, 2, 5))
    local consumed = len + 1
    if #s < consumed then
        -- need more data
        return nil, nil, true
    end

    -- CopyDone message has no data beyond the length field
    if len ~= 4 then
        return nil,
               errorf('invalid CopyDone message: unexpected length %d', len)
    end

    local msg = CopyDone()
    msg.consumed = consumed
    msg.type = 'CopyDone'

    return msg
end

return {
    encode = encode,
    decode = decode,
}
