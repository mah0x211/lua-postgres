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

--- @class postgres.message.parameter_status : postgres.message
--- @field name string
--- @field value string
local ParameterStatus = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- ParameterStatus (B)
    --   Byte1('S')
    --     Identifies the message as a run-time parameter status report.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   String
    --     The name of the run-time parameter being reported.
    --
    --   String
    --     The current value of the parameter.
    --
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'S' then
        return nil, errorf('invalid ParameterStatus message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if #s < consumed then
        return nil, nil, true
    end

    local head = 6
    local kv = {}
    for i = 1, 2 do
        -- find null-terminated string
        local tail = find(s, NULL, head, true)
        if not tail or tail > consumed then
            return nil, errorf('invalid ParameterStatus message')
        end
        kv[i] = sub(s, head, tail - 1)
        head = tail + 1
    end
    if head - consumed ~= 1 then
        -- message is not consumed correctly
        return nil, errorf('invalid ParameterStatus message')
    end

    local msg = ParameterStatus()
    msg.consumed = consumed
    msg.type = 'ParameterStatus'
    msg.name = kv[1]
    msg.value = kv[2]
    return msg
end

return {
    decode = decode,
}
