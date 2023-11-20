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
local concat = table.concat
local type = type
local htonl = require('postgres.htonl')
local htons = require('postgres.htons')
--- constants
local NULL = '\0'

--- encode
--- @param portal string
--- @param stmt string
--- @param values string[]
--- @return string
local function encode(portal, stmt, values)
    assert(type(portal) == 'string', 'portal must be string')
    assert(type(stmt) == 'string', 'stmt must be string')
    assert(type(values) == 'table', 'values must be table')
    --
    -- Bind (F)
    --   Byte1('B')
    --     Identifies the message as a Bind command.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   String
    --     The name of the destination portal (an empty string selects the
    --     unnamed portal).
    --
    --   String
    --     The name of the source prepared statement (an empty string selects
    --     the unnamed prepared statement).
    --
    --   Int16
    --     The number of parameter format codes that follow (denoted C below).
    --     This can be zero to indicate that there are no parameters or that the
    --     parameters all use the default format (text); or one, in which case
    --     the specified format code is applied to all parameters; or it can
    --     equal the actual number of parameters.
    --
    --   Int16[C]
    --     The parameter format codes. Each must presently be zero (text) or
    --     one (binary).
    --
    --   Int16
    --     The number of parameter values that follow (possibly zero). This must
    --     match the number of parameters needed by the query.
    --
    -- Next, the following pair of fields appear for each parameter:
    --
    --   Int32
    --     The length of the parameter value, in bytes (this count does not
    --     include itself). Can be zero. As a special case, -1 indicates a NULL
    --     parameter value. No value bytes follow in the NULL case.
    --
    --   Byten
    --     The value of the parameter, in the format indicated by the associated
    --     format code. n is the above length.
    --
    -- After the last parameter, the following fields appear:
    --
    --   Int16
    --     The number of result-column format codes that follow (denoted R
    --     below). This can be zero to indicate that there are no result columns
    --     or that the result columns should all use the default format (text);
    --     or one, in which case the specified format code is applied to all
    --     result columns (if any); or it can equal the actual number of result
    --     columns of the query.
    --
    --   Int16[R]
    --     The result-column format codes. Each must presently be zero (text) or
    --     one (binary).
    --
    local tbl = {
        portal,
        NULL,
        stmt,
        NULL,
        htons(0), -- number of parameter format codes
    }
    tbl[#tbl + 1] = htons(#values) -- number of parameter values
    for i = 1, #values do
        tbl[#tbl + 1] = htonl(#values[i])
        tbl[#tbl + 1] = values[i]
    end
    tbl[#tbl + 1] = htons(0) -- all result columns use the default format (text)

    local msg = concat(tbl)
    return 'B' .. htonl(#msg + 4) .. msg
end

return {
    encode = encode,
}
