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
local htonl = require('postgres.htonl')

--
-- SASLResponse (F)
--   Byte1('p')
--     Identifies the message as a SASL response. Note that this is also used
--     for GSSAPI, SSPI and password response messages. The exact message type
--     can be deduced from the context.
--
--   Int32
--     Length of message contents in bytes, including self.
--
--   Byten
--     SASL mechanism specific message data.
--

--- encode
--- @param resp string? SASL mechanism specific "Initial Client Response"
--- @return string
local function encode(resp)
    assert(type(resp) == 'string', 'resp must be string')
    return 'p' .. htonl(4 + #resp) .. resp
end

return {
    encode = encode,
}
