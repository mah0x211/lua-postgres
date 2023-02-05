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
--- @class postgres.rows
--- @field res postgres.result
--- @field nrow integer
--- @field fields table<string|integer, table<string, any>>
--- @field rowi integer
local Rows = {}

--- init
--- @param res postgres.result
--- @param nrow integer
--- @param fields table<string|integer, table<string, any>>
--- @return postgres.rows
function Rows:init(res, nrow, fields)
    self.res = res
    self.nrow = nrow
    self.fields = fields
    self.rowi = 1
    return self
end

--- close
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Rows:close()
    return self.res:close()
end

--- result
--- @return postgres.result res
function Rows:result()
    return self.res
end

--- read specified column value
--- @param col integer|string column name, or column number started with 1
--- @return string? val
--- @return table? field
function Rows:read(col)
    local field = self.fields[col]
    if field then
        local v = self.res:value(self.rowi, field.col)
        if v then
            return v, field
        end
    end
end

--- next retrives the next row
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Rows:next()
    if self.rowi < self.nrow then
        -- set to next row index
        self.rowi = self.rowi + 1
        return true
    end
    return false
end

return {
    new = require('metamodule').new(Rows),
}

