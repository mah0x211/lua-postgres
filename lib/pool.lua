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
local select = select
local type = type
local new_deadline = require('time.clock.deadline').new
local errorf = require('error').format
local instanceof = require('metamodule').instanceof
local parse_conninfo = require('postgres.conninfo')
local new_queue = require('postgres.pool.queue').new
local new_connection = require('postgres.pool.connection').new

--- @class postgres.pool
--- @field private queue denque
--- @field private maxconn integer max number of connections
--- @field private maxidle integer max number of idle connections
--- @field private chkintvl number interval to check alive in seconds
--- @field private queue_used postgres.pool.queue
--- @field private queue_idle postgres.pool.queue
local Pool = {}

--- init
--- @param maxconn integer?
--- @param maxidle integer?
--- @param chkintvl number?
--- @return postgres.pool
function Pool:init(maxconn, maxidle, chkintvl)
    -- max number of connections (default no-limit)
    assert(maxconn == nil or type(maxconn) == 'number' and maxconn == maxconn,
           'maxconn must be number or nil')
    maxconn = maxconn and maxconn >= 1 and maxconn < math.huge and
                  math.floor(maxconn) or 0

    -- max number of idle connections (default maxconn / 3 or 0)
    if maxidle then
        assert(type(maxidle) == 'number' and maxidle == maxidle and
                   (maxconn <= 0 or maxidle <= maxconn),
               'maxidle must be nil or number less than or equal to maxconn')
        maxidle = maxidle > 0 and math.floor(maxidle) or 0
    else
        maxidle = maxconn > 0 and math.floor(maxconn / 3) or 0
    end

    -- check interval in seconds (default 30)
    assert(chkintvl == nil or type(chkintvl) == 'number' and chkintvl ==
               chkintvl and chkintvl >= 0,
           'chkintvl must be positive number or nil')
    self.chkintvl = math.floor(chkintvl or 30)

    self.maxconn = maxconn
    self.maxidle = maxidle
    self.queue_used = new_queue()
    self.queue_idle = new_queue()
    return self
end

--- shutdown all unused connections.
--- after shutdown, the pool cannot hold unused connections.
--- @return integer nconn
function Pool:shutdown()
    local nconn = 0
    local conn = self.queue_idle:shift()
    while conn do
        nconn = nconn + 1
        conn:close()
        conn = self.queue_idle:shift()
    end
    self.queue_idle = nil
    return nconn
end

--- close all connections.
--- after close, the pool cannot hold any connections.
--- @return integer nconn
function Pool:close()
    local nconn = 0
    if self.queue_idle then
        nconn = self:shutdown()
    end

    if self.queue_used then
        local conn = self.queue_used:shift()
        while conn do
            nconn = nconn + 1
            conn:close()
            conn = self.queue_used:shift()
        end
        self.queue_used = nil
    end

    return nconn
end

--- size
--- @return integer
function Pool:size()
    return self:size_used() + self:size_idle()
end

--- size_used
--- @return integer
function Pool:size_used()
    return self.queue_used and self.queue_used:size() or 0
end

--- size_idle
--- @return integer
function Pool:size_idle()
    return self.queue_idle and self.queue_idle:size() or 0
end

--- get
--- @param conninfo string?
--- @return postgres.pool.connection? conn
--- @return any err
--- @return boolean? again
--- @return boolean? timeout
function Pool:get(conninfo)
    assert(conninfo == nil or type(conninfo) == 'string',
           'conninfo must be string or nil')

    if not self.queue_idle then
        return nil, errorf(
                   'connections cannot be retrieved from closed or shutdown pool')
    end

    if not conninfo then
        conninfo = select(3, parse_conninfo(''))
    end

    -- get connection from the idle queue
    local conn = self.queue_idle:pop(conninfo)
    while conn do
        -- check connection is alive
        if conn:checkalive() then
            -- push to the used queue
            self.queue_used:push(conn)
            return conn
        end
        -- not alive
        conn:close()

        -- get next connection
        conn = self.queue_idle:pop(conninfo)
    end

    -- remove the oldest connection from the idle queue
    if self.maxconn > 0 and self:size() >= self.maxconn then
        conn = self.queue_idle:shift()
        if not conn then
            -- pool is full
            return nil, nil, true
        end
        -- close the connection
        conn:close()
    end

    -- create new connection
    local err, timeout
    conn, err, timeout = new_connection(conninfo)
    if not conn then
        return nil, err, nil, timeout
    end
    -- push to the used queue
    self.queue_used:push(conn)

    return conn
end

--- release
--- @param conn postgres.pool.connection
--- @param destroy boolean?
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Pool:release(conn, destroy)
    assert(instanceof(conn, 'postgres.pool.connection'),
           'conn must be postgres.pool.connection')
    assert(destroy == nil or type(destroy) == 'boolean',
           'destroy must be boolean or nil')

    -- just close connection if pool is closed
    if not self.queue_used then
        conn:close()
        return true
    end

    -- remove from the used queue
    assert(self.queue_used:remove(conn),
           'connection is not managed by this pool')
    -- close connection if destroy argument is true or pool is shutdown
    if destroy or not self.queue_idle then
        conn:close()
        return true
    end

    -- waits connection to be ready for query
    local ok, err, timeout = conn:wait_ready()
    if not ok then
        -- connection closed by server
        return false, err, timeout
    end

    -- push to the idle queue
    self.queue_idle:push(conn)
    while self.queue_idle:size() > self.maxidle do
        -- remove the oldest connection from the idle queue
        --- @type postgres.pool.connection
        conn = self.queue_idle:shift()
        conn:close()
    end
    return true
end

--- evict idle connections
--- @param sec? number seconds to wait for evicting (0 means evict all connections)
--- @return integer n number of evicted connections
--- @return boolean? timeout
function Pool:evict(sec)
    assert(sec == nil or (type(sec) == 'number' and sec >= 0),
           'sec must be unsigned number or nil')

    local deadline = sec and new_deadline(sec)
    local queue_idle = self.queue_idle
    local nconn = 0
    for _ = 1, queue_idle:size() do
        --- @type postgres.pool.connection
        local conn = self.queue_idle:shift()
        if not conn:checkalive(self.chkintvl) then
            -- close the connection is not alive
            conn:close()
            nconn = nconn + 1
        else
            -- push back to the idle queue
            queue_idle:push(conn)
        end

        -- stop eviction if deadline is expired
        if deadline and deadline:remain() <= 0 then
            return nconn, true
        end
    end

    return nconn
end

return {
    new = require('metamodule').new(Pool),
}

