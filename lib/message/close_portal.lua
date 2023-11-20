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
--- @return string
local function encode(portal)
    assert(type(portal) == 'string', 'portal must be string')
    --
    -- Close (F)
    --   Byte1('C')
    --     Identifies the message as a Close command.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Byte1
    --     'S' to close a prepared statement; or 'P' to close a portal.
    --
    --   String
    --     The name of the prepared statement or portal to close (an empty
    --     string selects the unnamed prepared statement or portal).
    --
    return 'C' .. htonl(4 + 1 + #portal + 1) .. 'P' .. portal .. NULL
end

return {
    encode = encode,
}
