--
-- Copyright (C) 2022 Masatoshi Fukunaga
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
local wait = require('io.wait')
local io_readable = wait.readable
local io_writable = wait.writable
local poll = require('gpoll')
local pollable = poll.pollable
local poll_readable = poll.readable
local poll_writable = poll.writable
local libpq = require('libpq')
--- constants
local DEFAULT_DEADLINE = 3000

--- @class postgres
--- @field conn libpq.conn
local Postgres = {}

--- init
--- @param conn libpq.conn
--- @return postgres
function Postgres:init(conn)
    self.conn = conn
    return self
end

--- wait_readable
--- @param deadline integer
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Postgres:wait_readable(deadline)
    local wait_readable = pollable() and poll_readable or io_readable
    -- wait until readable
    return wait_readable(self.conn:socket(), deadline or DEFAULT_DEADLINE)
end

--- wait_writable
--- @param deadline integer
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Postgres:wait_writable(deadline)
    local wait_writable = pollable() and poll_writable or io_writable
    -- wait until writable
    return wait_writable(self.conn:socket(), deadline or DEFAULT_DEADLINE)
end

require('metamodule').new(Postgres)

local _M = {}
-- export constants
for k, v in pairs(libpq) do
    if string.find(k, '^[A-Z_]+$') then
        _M[k] = v
    end
end

-- export functions
_M.is_threadsafe = libpq.is_threadsafe
_M.unescape_bytea = libpq.unescape_bytea
_M.lib_version = libpq.lib_version
_M.mblen = libpq.mblen
_M.mblen_bounded = libpq.mblen_bounded
_M.dsplen = libpq.dsplen
_M.env2encoding = libpq.env2encoding
_M.encrypt_password = libpq.encrypt_password
_M.char_to_encoding = libpq.char_to_encoding
_M.encoding_to_char = libpq.encoding_to_char
_M.valid_server_encoding_id = libpq.valid_server_encoding_id

return _M
