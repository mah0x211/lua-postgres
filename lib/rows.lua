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
local type = type
local errorf = require('error').format
local instanceof = require('metamodule').instanceof
local DEFAULT_DECODER = require('postgres.decoder').new()

--- @class postgres.rows
--- @field private conn postgres.connection?
--- @field private coli integer
--- @field private row table? DataRow message
--- @field fields table RowDescription.fields
--- @field error string?
--- @field is_timeout boolean?
--- @field complete postgres.message.command_complete?
local Rows = {}

--- init
--- @param conn postgres.connection
--- @param fields table RowDescription fields
--- @return postgres.rows
function Rows:init(conn, fields)
    assert(instanceof(conn, 'postgres.connection'),
           'conn must be a postgres.connection')
    assert(type(fields) == 'table')
    self.conn = conn
    self.coli = 1
    self.fields = fields
    return self
end

--- close
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Rows:close()
    local conn = self.conn
    if not conn then
        return true
    end

    -- remove connection and row
    self.conn = nil
    self.row = nil
    -- retrieve CommandComplete message
    while not self.complete do
        -- the allowed message types are;
        --  * DataRow
        --  * CommandComplete
        --  * ErrorResponse
        local res, err, timeout = conn:recv()
        if not res then
            if err then
                self.error = errorf('failed to retrieve message: %s', err)
            end
            self.is_timeout = timeout
            return false, self.error, timeout
        end

        if res.type == 'CommandComplete' then
            self.complete = res
        elseif res.type == 'ErrorResponse' then
            self.error = errorf('[%s] %s', res.severity, res.message)
        elseif res.type ~= 'DataRow' then
            self.error = errorf(
                             'DataRow|CommandComplete|ErrorResponse expected, got %q',
                             res.type)
        end
    end

    -- retrieve ReadyForQuery message
    return conn:wait_ready()
end

--- next retrives the DataRow message
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Rows:next()
    local conn = self.conn
    if not conn or self.complete or self.error then
        return false
    end

    -- remove current row
    self.row = nil
    -- retrieve next row
    -- the allowed message types are;
    --  * DataRow
    --  * CommandComplete
    --  * ErrorResponse
    local res, err, timeout = conn:recv()
    if not res then
        if err then
            self.error = errorf('failed to retrieve message: %s', err)
        end
        self.is_timeout = timeout
        self.conn = nil
        return false, self.error, timeout
    end

    if res.type == 'DataRow' then
        -- set current row and reset column index
        self.row = res
        self.coli = 1
        return true
    elseif res.type == 'CommandComplete' then
        self.complete = res
        return false
    elseif res.type == 'ErrorResponse' then
        self.error = errorf('[%s] %s', res.severity, res.message)
        return false, self.error
    else
        self.error = errorf(
                         'DataRow|CommandComplete|ErrorResponse expected, got %q',
                         res.type)
        return false, self.error
    end
end

--- readat read specified column value
--- @param col integer|string column name, or column number started with 1
--- @return table? field
--- @return string? val
function Rows:readat(col)
    local field = self.row and self.fields[col]
    if field then
        return field, self.row.values[field.col]
    end
end

--- read read next column value
--- @return table? field
--- @return string? val
function Rows:read()
    local field = self.row and self.fields[self.coli]
    if field then
        self.coli = self.coli + 1
        return field, self.row.values[field.col]
    end
end

--- scanat scan specified column value
--- @param col integer|string column name, or column number started with 1
--- @param decoder? postgres.decoder
--- @return table? field
--- @return any val
--- @return any err
function Rows:scanat(col, decoder)
    if decoder == nil then
        decoder = DEFAULT_DECODER
    end

    local field, val = self:readat(col)
    if field and val then
        return field, decoder:decode_by_oid(field.type_oid, val)
    end
    return field
end

--- scan scan next column value
--- @param decoder? postgres.decoder
--- @return table? field
--- @return any val
--- @return any err
function Rows:scan(decoder)
    if decoder == nil then
        decoder = DEFAULT_DECODER
    end

    local field, val = self:read()
    if field and val then
        return field, decoder:decode_by_oid(field.type_oid, val)
    end
    return field
end

return {
    new = require('metamodule').new(Rows),
}

