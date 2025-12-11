--
-- Copyright (C) 2025 Masatoshi Fukunaga
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
local byte = string.byte
local sub = string.sub
local errorf = require('error').format
local ntohl = require('postgres.ntohl')
local ntohs = require('postgres.ntohs')

--- @class postgres.message.copy_both_response : postgres.message
--- @field format 'text' | 'binary'
--- @field column_count integer
local CopyBothResponse = {}

CopyBothResponse = require('metamodule').new(CopyBothResponse,
                                             'postgres.message')

--- decode
--- @param s string
--- @return postgres.message.copy_both_response? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- CopyBothResponse (B)
    --   Byte1('W')
    --     Identifies the message as a Start Copy Both response.
    --     This message is used only for Streaming Replication.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int8
    --     0 indicates the overall COPY format is textual (rows separated by
    --     newlines, columns separated by separator characters, etc.).
    --     1 indicates the overall copy format is binary (similar to DataRow
    --     format). See COPY for more information.
    --
    --   Int16
    --     The number of columns in the data to be copied (denoted N below).
    --
    --   Int16[N]
    --     The format codes to be used for each column. Each must presently be
    --     zero (text) or one (binary). All must be zero if the overall copy
    --     format is textual.
    --
    if #s < 8 then
        -- need more data for header
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'W' then
        return nil, errorf('invalid CopyBothResponse message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if #s < consumed then
        -- need more data
        return nil, nil, true
    end

    -- overall copy format (0 = text, 1 = binary)
    local format = byte(sub(s, 6, 6))
    if format ~= 0 and format ~= 1 then
        return nil, errorf(
                   'invalid CopyBothResponse message: unsupported copy format %d',
                   format)
    end

    -- check expected length for column format codes
    local ncol = ntohs(sub(s, 7))
    local expected_len_without_type = 7 + (ncol * 2)
    if len ~= expected_len_without_type then
        return nil, errorf(
                   'invalid CopyBothResponse message: length mismatch, expected %d but got %d',
                   expected_len_without_type, len)
    end

    -- all column formats must be same as overall format
    local head = 9
    for i = 1, ncol do
        local col_format = ntohs(sub(s, head))
        if col_format ~= format then
            return nil, errorf(
                       'invalid CopyBothResponse message: column#%d format %d does not match overall format %d',
                       i, col_format, format)
        end
        head = head + 2
    end

    local msg = CopyBothResponse()
    msg.consumed = consumed
    msg.type = 'CopyBothResponse'
    msg.format = format == 0 and 'text' or 'binary'
    msg.column_count = ncol
    return msg
end

return {
    decode = decode,
}
