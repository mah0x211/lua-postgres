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
local next = next

--- @class denque
--- @field __len fun(self):integer
--- @field unshift fun(self, data:any):(elm:denque.element)
--- @field shift fun(self):(data:any)
--- @field push fun(self, data:any):(elm:denque.element)
--- @field pop fun(self):(data:any)
--- @field head fun(self):(elm:denque.element?)
--- @field tail fun(self):(elm:denque.element?)
--- @field remove fun(self, elm:denque.element):(data:any)
--- @class denque.element
--- @field data fun(self, newdata?:any):(data:any)
--- @field prev fun(self):(elm:denque.element?)
--- @field next fun(self):(elm:denque.element?)
--- @field remove fun(self):(data:any)

--- @type fun():denque
local new_denque = require('denque').new

--- @class postgres.pool.queue
--- @field private queue denque
--- @field private addr2elms table<string, denque.element[]>
local Queue = {}

--- init
function Queue:init()
    self.queue = new_denque()
    self.addr2elms = {}
    return self
end

--- size
--- @return integer
function Queue:size()
    return #self.queue
end

--- push
--- @param conn postgres.pool.connection
function Queue:push(conn)
    local addr = conn:get_conninfo()
    local elms = self.addr2elms[addr]
    if not elms then
        -- create new list for the conninfo
        elms = {}
        self.addr2elms[addr] = elms
    end
    local idx = #elms + 1
    conn:set_pool_id(idx)
    elms[idx] = self.queue:push(conn)
end

--- shift
--- @return postgres.pool.connection?
function Queue:shift()
    -- remove oldest element from the queue
    local conn = self.queue:shift()
    if conn then
        -- remove from list
        local addr = conn:get_conninfo()
        local elms = self.addr2elms[addr]
        elms[conn:get_pool_id()] = nil
        if not next(elms) then
            -- remove empty list
            self.addr2elms[addr] = nil
        end
        return conn
    end
end

--- pop
--- @param addr string
--- @return postgres.pool.connection? conn
function Queue:pop(addr)
    local elms = self.addr2elms[addr]
    local elm = elms and elms[#elms]
    if elm then
        -- remove from list
        local conn = elm:remove()
        elms[conn:get_pool_id()] = nil
        if not next(elms) then
            -- remove empty list
            self.addr2elms[addr] = nil
        end
        return conn
    end
end

--- remove
--- @param conn postgres.pool.connection
--- @return boolean ok
function Queue:remove(conn)
    local addr = conn:get_conninfo()
    local idx = conn:get_pool_id()
    local elms = self.addr2elms[addr]
    local elm = elms and elms[idx]
    if not elm then
        return false
    end

    -- remove from list
    assert(elm:remove() == conn)
    elms[idx] = nil
    if not next(elms) then
        -- remove empty list
        self.addr2elms[addr] = nil
    end
    return true
end

return {
    new = require('metamodule').new(Queue),
}
