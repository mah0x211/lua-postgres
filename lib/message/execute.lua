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
--- constants
local NULL = '\0'

--- encode
--- @param portal string
--- @param max_rows integer?
--- @return string
local function encode(portal, max_rows)
    assert(type(portal) == 'string', 'portal must be string')
    assert(max_rows == nil or type(max_rows) == 'number',
           'max_rows must be integer or nil')
    --
    -- Execute (F)
    --   Byte1('E')
    --     Identifies the message as an Execute command.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   String
    --     The name of the portal to execute (an empty string selects the
    --     unnamed portal).
    --
    --   Int32
    --     Maximum number of rows to return, if portal contains a query that
    --     returns rows (ignored otherwise). Zero denotes “no limit”.
    --
    return 'E' .. htonl(4 + #portal + 1 + 4) .. portal .. NULL ..
               htonl(max_rows or 0)
end

return {
    encode = encode,
}
