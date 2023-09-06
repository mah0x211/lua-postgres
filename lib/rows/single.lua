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
local is_finite = isa.finite

--- @class postgres.rows.single : postgres.rows
--- @field private done? boolean
--- @field is_timeout? boolean
--- @field error any
local SingleRows = {}

--- next retrives the next row
--- @param sec? number
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function SingleRows:next(sec)
    assert(sec == nil or is_finite(sec), 'sec must be finite number or nil')
    if self.done then
        return false
    elseif self.rowi ~= 1 then
        self.rowi = 1
        return true
    end

    local res
    res, self.error, self.is_timeout = self.res:next(sec)
    if not res then
        return false, self.error, self.is_timeout
    elseif res:status() ~= 'single_tuple' then
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

