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

--- @class postgres.message.ready_for_query : postgres.message
--- @field status string 'idle' | 'transaction' | 'failed_transaction'
local ReadyForQuery = require('metamodule').new({}, 'postgres.message')

--- constants
local STATUS = {
    I = 'idle',
    T = 'transaction',
    E = 'failed_transaction',
}

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- ReadyForQuery (B)
    --   Byte1('Z')
    --     Identifies the message type. ReadyForQuery is sent whenever the
    --     backend is ready for a new query cycle.
    --
    --   Int32(5)
    --     Length of message contents in bytes, including self.
    --
    --   Byte1
    --     Current backend transaction status indicator. Possible values are
    --      'I' if idle (not in a transaction block);
    --      'T' if in a transaction block;
    --      or 'E' if in a failed transaction block (queries will be rejected
    --      until block is ended).
    --
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'Z' then
        return nil, errorf('invalid ReadyForQuery message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if len ~= 5 then
        return nil, errorf('invalid ReadyForQuery message')
    elseif #s < consumed then
        return nil, nil, true
    end

    local code = sub(s, 6, 6)
    local status = STATUS[code]
    if not status then
        return nil, errorf(
                   'invalid ReadyForQuery message: unsupported status %q', code)
    end

    local msg = ReadyForQuery()
    msg.consumed = consumed
    msg.type = 'ReadyForQuery'
    msg.status = status
    return msg
end

return {
    decode = decode,
}
