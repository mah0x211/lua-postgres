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
local type = type
local htonl = require('postgres.htonl')
local htons = require('postgres.htons')
--- constants
local NULL = '\0'

--- encode
--- @param stmt string
--- @param query string
--- @return string
local function encode(stmt, query)
    assert(type(stmt) == 'string', 'stmt must be string')
    assert(type(query) == 'string', 'query must be string')
    --
    -- Parse (F)
    --   Byte1('P')
    --     Identifies the message as a Parse command.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   String
    --     The name of the destination prepared statement (an empty string
    --     selects the unnamed prepared statement).
    --
    --   String
    --     The query string to be parsed.
    --
    --   Int16
    --     The number of parameter data types specified (can be zero). Note that
    --     this is not an indication of the number of parameters that might
    --     appear in the query string, only the number that the frontend wants
    --     to prespecify types for.
    --
    --   Then, for each parameter, there is the following:
    --
    --   Int32
    --     Specifies the object ID of the parameter data type. Placing a zero
    --     here is equivalent to leaving the type unspecified.
    --
    return
        'P' .. htonl(4 + #stmt + 1 + #query + 1 + 2) .. stmt .. NULL .. query ..
            NULL .. htons(0)
end

return {
    encode = encode,
}
