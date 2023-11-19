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

--- @class postgres.message
--- @field consumed integer?
--- @field conn postgres.connection?
--- @field type string
local Message = {}

--- next retrieves the next message from the connection.
--- @return postgres.message? msg
--- @return any err
--- @return boolean? timeout
function Message:next()
    if not self.conn then
        return nil
    end
    return self.conn:next()
end

require('metamodule').new(Message)

local DECODER = {
    R = require('postgres.message.authentication').decode,
    K = require('postgres.message.backend_key_data').decode,
}

--- decode_message
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    if #s < 1 then
        return nil, nil, true
    end

    local decoder = DECODER[sub(s, 1, 1)]
    if not decoder then
        return nil, errorf('unknown message type')
    end
    return decoder(s)
end

return {
    decode = decode,
}