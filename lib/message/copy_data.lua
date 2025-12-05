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
-- CopyData (F & B)
--   Byte1('d')
--     Identifies the message as CopyData.
--
--   Int32
--     Length of message contents in bytes, including self.
--
--   Byten
--     Data that forms part of a CopyData message.  Messages are sent from a
--     backend during a COPY TO operation, or sent by a frontend during a COPY
--     FROM operation.  Messages are sent until a CopyDone message is sent.
--     Optionally, a frontend can issue a CopyFail message instead of CopyDone
--     to signal failure of the copy operation.
--

--- @class postgres.message.copy_data : postgres.message
--- @field data string
local CopyData = require('metamodule').new({}, 'postgres.message')

--- encode
--- @param data string
--- @return string msg
local function encode(data)
    local len = 4 + #data -- length field + data
    return 'd' .. htonl(len) .. data
end

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'd' then
        return nil, errorf('invalid CopyData message')
    end

    local len = ntohl(sub(s, 2, 5))
    local consumed = len + 1

    if #s < consumed then
        return nil, nil, true
    end

    local msg = CopyData()
    msg.consumed = consumed
    msg.type = 'CopyData'
    msg.data = sub(s, 6, consumed)

    return msg
end

return {
    encode = encode,
    decode = decode,
}
