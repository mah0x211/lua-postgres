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
local DEFAULT_DECODER = require('postgres.decoder').new()

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
    self.rowi = 0
    self.coli = 1
    return self
end

--- close
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Rows:close()
    return self.res:close()
end

--- next retrives the next row
--- @return boolean ok
function Rows:next()
    if self.rowi < self.nrow then
        -- set to next row index
        self.rowi = self.rowi + 1
        -- reset column position
        self.coli = 1
        return true
    end
    return false
end

--- result
--- @return postgres.result res
function Rows:result()
    return self.res
end

--- readat read specified column value
--- @param col integer|string column name, or column number started with 1
--- @return table? field
--- @return string? val
function Rows:readat(col)
    local field = self.rowi > 0 and self.fields[col]
    if field then
        return field, self.res:value(self.rowi, field.col)
    end
end

--- read read next column value
--- @return table? field
--- @return string? val
function Rows:read()
    local field = self.rowi > 0 and self.fields[self.coli]
    if field then
        self.coli = self.coli + 1
        return field, self.res:value(self.rowi, field.col)
    end
end

--- scanat scan specified column value
--- @param col integer|string column name, or column number started with 1
--- @param decoder? postgres.decoder
--- @return any val
--- @return any err
--- @return table? field
function Rows:scanat(col, decoder)
    if decoder == nil then
        decoder = DEFAULT_DECODER
    end

    local field, val = self:readat(col)
    if field and val then
        local dval, err = decoder:decode_by_oid(field.type, val)
        return dval, err, field
    end
end

--- scan scan next column value
--- @param decoder? postgres.decoder
--- @return any val
--- @return any err
--- @return table? field
function Rows:scan(decoder)
    if decoder == nil then
        decoder = DEFAULT_DECODER
    end

    local field, val = self:read()
    if field and val then
        local dval, err = decoder:decode_by_oid(field.type, val)
        return dval, err, field
    end
end

return {
    new = require('metamodule').new(Rows),
}

