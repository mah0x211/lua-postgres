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

--- @class postgres.reader.single : postgres.reader
--- @field done? boolean
--- @field err? error
--- @field timeout? boolean
local SingleReader = {}

--- result
--- @return postgres.result res
--- @return error err
--- @return boolean timeout
function SingleReader:result()
    return self.res, self.err, self.timeout
end

--- rows
--- @param deadline? integer
--- @return function iter
function SingleReader:read(deadline)
    if deadline ~= nil and not is_uint(deadline) then
        error('deadline must be uint', 2)
    elseif self.done then
        -- do nothing
        return function()
        end
    end

    local reader = self
    local res = reader.res
    local stat = res:stat()
    local fields = stat.fields
    local ncol = stat.nfields
    local rowi = 1
    local coli = 0

    return function()
        coli = coli + 1
        if coli > ncol then
            -- set to next row index after read all columns
            coli = 1
            rowi = rowi + 1
            reader.rowi = rowi

            local err, timeout
            res, err, timeout = res:next(deadline)
            if not res then
                reader.err = err
                reader.timeout = timeout
                return nil
            end

            -- clear current result and replace it with new result
            reader.res:clear()
            reader.res = res

            local status = res:status()
            if status ~= PGRES_SINGLE_TUPLE then
                -- done
                reader.done = true
                return nil
            end
        end

        local v = res:value(1, coli)
        return rowi, fields[coli], v
    end
end

return {
    new = require('metamodule').new(SingleReader, 'postgres.reader'),
}

