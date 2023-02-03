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
--- @class postgres.reader
--- @field res postgres.result
--- @field rowi? integer
local Reader = {}

--- init
--- @return postgres.reader
function Reader:init(res)
    self.res = res
    return self
end

--- close
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Reader:close()
    return self.res:close()
end

--- result
--- @return postgres.result res
function Reader:result()
    return self.res
end

--- read
--- @return function iter
function Reader:read()
    local reader = self
    local res = reader.res
    local stat = res:stat()
    local fields = stat.fields
    local nrow = stat.ntuples
    local ncol = stat.nfields
    local rowi = self.rowi or 1
    local coli = 0

    return function()
        coli = coli + 1
        if coli > ncol then
            -- set to next row index after read all columns
            coli = 1
            rowi = rowi + 1
            reader.rowi = rowi
        end

        if rowi > nrow then
            res:clear()
            return nil
        end

        local v = res:value(rowi, coli)
        return rowi, fields[coli], v
    end
end

return {
    new = require('metamodule').new(Reader),
}

