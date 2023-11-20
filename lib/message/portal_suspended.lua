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

--- @class postgres.message.portal_suspended : postgres.message
local PortalSuspended = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- PortalSuspended (B)
    --   Byte1('s')
    --     Identifies the message as a portal-suspended indicator. Note this
    --     only appears if an Execute message's row-count limit was reached.
    --
    --   Int32(4)
    --     Length of message contents in bytes, including self.
    --
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 's' then
        return nil, errorf('invalid PortalSuspended message')
    end

    local len = ntohl(sub(s, 2))
    if len ~= 4 then
        return nil, errorf('invalid PortalSuspended message')
    end

    local msg = PortalSuspended()
    msg.consumed = len + 1
    msg.type = 'PortalSuspended'
    return msg
end

return {
    decode = decode,
}
