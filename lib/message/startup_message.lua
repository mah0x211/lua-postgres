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
local htonl = require('postgres.htonl')
--- constants
local NULL = '\0'

--- encode
--- @param params table<string, string>
--- @return string
local function encode(params)
    --
    -- StartupMessage (F)
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32(196608)
    --     The protocol version number. The most significant 16 bits are the
    --     major version number (3 for the protocol described here). The least
    --     significant 16 bits are the minor version number (0 for the protocol
    --     described here).
    --
    -- The protocol version number is followed by one or more pairs of parameter
    -- name and value strings. A zero byte is required as a terminator after the
    -- last name/value pair. Parameters can appear in any order. user is
    -- required, others are optional. Each parameter is specified as:
    --
    --   String
    --     The parameter name. Currently recognized names are;
    --
    --     user
    --       The database user name to connect as. Required; there is no default.
    --     database
    --       The database to connect to. Defaults to the user name.
    --     application_name
    --       Sets the application name parameter of the connection.
    --
    local tbl = {
        htonl(196608),
    }
    -- add params
    for k, v in pairs(params) do
        tbl[#tbl + 1] = k
        tbl[#tbl + 1] = NULL
        tbl[#tbl + 1] = v
        tbl[#tbl + 1] = NULL
    end
    -- terminator
    tbl[#tbl + 1] = NULL

    local msg = concat(tbl)
    return htonl(#msg + 4) .. msg
end

return {
    encode = encode,
}
