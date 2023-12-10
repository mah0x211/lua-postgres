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
local gettime = require('time.clock').gettime

--- @class postgres.pool.connection : postgres.connection
--- @field private pool_id integer
--- @field private checkalive_at number
local Connection = {}

--- init
--- @param conninfo? string
--- @return postgres.pool.connection?
--- @return any err
--- @return boolean? timeout
function Connection:init(conninfo)
    self.checkalive_at = gettime()
    return self['postgres.connection'].init(self, conninfo)
end

--- set_pool_id
--- @param id integer
function Connection:set_pool_id(id)
    self.pool_id = id
end

--- get_pool_id
--- @return integer
function Connection:get_pool_id()
    return self.pool_id
end

--- checkalive
--- @param chkintvl? number
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Connection:checkalive(chkintvl)
    assert(chkintvl == nil or (type(chkintvl) == 'number' and chkintvl >= 0),
           'chkintvl must be positive number or nil')

    local now = gettime()
    if not chkintvl or now - self.checkalive_at >= chkintvl then
        self.checkalive_at = now
        return self:ping()
    end
    return true
end

return {
    new = require('metamodule').new(Connection, 'postgres.connection'),
}
