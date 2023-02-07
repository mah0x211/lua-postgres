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
local isa = require('isa')
local is_uint = isa.uint
local libpq = require('libpq')
--- constants
local PGRES_SINGLE_TUPLE = libpq.PGRES_SINGLE_TUPLE

--- @class postgres.rows.single : postgres.rows
--- @field done? boolean
local SingleRows = {}

--- next retrives the next row
--- @param deadline? integer
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function SingleRows:next(deadline)
    if deadline ~= nil and not is_uint(deadline) then
        error('deadline must be uint', 2)
    elseif self.done then
        return false
    end

    local res, err, timeout = self.res:next(deadline)
    if not res then
        return false, err, timeout
    elseif res:status() ~= PGRES_SINGLE_TUPLE then
        -- done
        res:clear()
        self.done = true
        return false
    end

    -- clear current result and replace it with new result
    self.res:clear()
    self.res = res
    self.coli = 1
    return true
end

return {
    new = require('metamodule').new(SingleRows, 'postgres.rows'),
}

