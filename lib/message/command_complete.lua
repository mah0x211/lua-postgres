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
local sub = string.sub
local gmatch = string.gmatch
local errorf = require('error').format
local ntohl = require('postgres.ntohl')

--- @class postgres.message.command_complete : postgres.message
--- @field tag string
--- @field rows integer
local CommandComplete = require('metamodule').new({}, 'postgres.message')

local HAS_ROWS = {
    INSERT = true,
    DELETE = true,
    UPDATE = true,
    SELECT = true,
    MOVE = true,
    FETCH = true,
    COPY = true,
}

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- CommandComplete (B)
    --   Byte1('C')
    --     Identifies the message as a command-completed response.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   String
    --     The command tag. This is usually a single word that identifies which
    --     SQL command was completed.
    --
    --   For an INSERT command, the tag is INSERT oid rows, where rows is the
    --   number of rows inserted. oid used to be the object ID of the inserted
    --   row if rows was 1 and the target table had OIDs, but OIDs system
    --   columns are not supported anymore; therefore oid is always 0.
    --
    --   For a DELETE command, the tag is DELETE rows where rows is the number
    --   of rows deleted.
    --
    --   For an UPDATE command, the tag is UPDATE rows where rows is the number
    --   of rows updated.
    --
    --   For a SELECT or CREATE TABLE AS command, the tag is SELECT rows where
    --   rows is the number of rows retrieved.
    --
    --   For a MOVE command, the tag is MOVE rows where rows is the number of
    --   rows the cursor's position has been changed by.
    --
    --   For a FETCH command, the tag is FETCH rows where rows is the number of
    --   rows that have been retrieved from the cursor.
    --
    --   For a COPY command, the tag is COPY rows where rows is the number of
    --   rows copied. (Note: the row count appears only in PostgreSQL 8.2 and
    --   later.)
    --
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'C' then
        return nil, errorf('invalid CommandComplete message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if #s < consumed then
        return nil, nil, true
    end

    -- split tag with spaces
    local tag = sub(s, 6, consumed - 1)
    local words = {}
    for word in gmatch(tag, '%S+') do
        words[#words + 1] = word
    end

    local rows
    if not words[1] then
        return nil, errorf('invalid CommandComplete message: empty tag')
    elseif HAS_ROWS[words[1]] then
        tag = words[1]
        rows = tonumber(tag == 'INSERT' and words[3] or words[2])
    end

    local msg = CommandComplete()
    msg.consumed = consumed
    msg.type = 'CommandComplete'
    msg.tag = tag
    msg.rows = rows
    return msg
end

return {
    decode = decode,
}
