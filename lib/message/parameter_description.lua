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
local ntohs = require('postgres.ntohs')

--- @class postgres.message.parameter_description : postgres.message
--- @field type_oids integer[]
local ParameterDescription = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- ParameterDescription (B)
    --   Byte1('t')
    --     Identifies the message as a parameter description.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int16
    --     The number of parameters used by the statement (can be zero).
    --
    --   Then, for each parameter, there is the following:
    --
    --   Int32
    --     Specifies the object ID of the parameter data type.
    --
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 't' then
        return nil, errorf('invalid ParameterDescription message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if #s < consumed then
        return nil, nil, true
    end

    local nparam = ntohs(sub(s, 6))
    local oids = {}
    local head = 8
    for i = 1, nparam do
        oids[i] = ntohl(sub(s, head))
        head = head + 4
    end

    local msg = ParameterDescription()
    msg.consumed = consumed
    msg.type = 'ParameterDescription'
    msg.type_oids = oids
    return msg
end

return {
    decode = decode,
}
