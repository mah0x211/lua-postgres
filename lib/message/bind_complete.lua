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

--- @class postgres.message.bind_complete : postgres.message
local BindComplete = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- BindComplete (B)
    --   Byte1('2')
    --     Identifies the message as a Bind-complete indicator.
    --
    --   Int32(4)
    --     Length of message contents in bytes, including self.
    --
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= '2' then
        return nil, errorf('invalid BindComplete message')
    end

    local len = ntohl(sub(s, 2))
    if len ~= 4 then
        return nil, errorf('invalid BindComplete message')
    end

    local msg = BindComplete()
    msg.consumed = len + 1
    msg.type = 'BindComplete'
    return msg
end

return {
    decode = decode,
}
