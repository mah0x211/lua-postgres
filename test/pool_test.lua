require('luacov')
local testcase = require('testcase')
local new_connection = require('postgres.connection').new
local new_pool = require('postgres.pool').new

function testcase.new()
    -- test that create new pool
    local pool = assert(new_pool())
    assert.match(pool, 'postgres.pool')
end

function testcase.set_get()
    local pool = assert(new_pool())
    local c1 = assert(new_connection())
    local c2 = assert(new_connection())

    -- test that pool connection
    pool:set(c1)
    pool:set(c2)

    -- test that randomly get a connection from the pool
    local conns = {
        [c1] = true,
        [c2] = true,
    }
    for _ = 1, 2 do
        local c = assert(pool:get())
        assert.is_true(conns[c])
        conns[c] = nil
        c:close()
    end
    assert.empty(conns)

    -- test that return nil if pool is empty
    assert.is_nil(pool:get())
end

function testcase.clear()
    local pool = assert(new_pool())
    local c1 = assert(new_connection())
    local c2 = assert(new_connection())
    local c3 = assert(new_connection())
    pool:set(c1)
    pool:set(c2)
    pool:set(c3)

    -- test that clear pooled connection
    assert.equal(pool:clear(), 3)
    for _, c in ipairs({
        c1,
        c2,
        c3,
    }) do
        local err = assert.throws(function()
            c:status()
        end)
        assert.match(err, 'attempt to use a freed object')
    end

    -- test that clear specified number of pooled connection
    c1 = assert(new_connection())
    c2 = assert(new_connection())
    pool:set(c1)
    pool:set(c2)
    assert.equal(pool:clear(nil, 1), 1)
    c1 = assert(pool:get())
    assert.is_nil(pool:get())

    -- test that clear if callback returns true
    c2 = assert(new_connection())
    c3 = assert(new_connection())
    pool:set(c1)
    pool:set(c2)
    pool:set(c3)
    local n = 0
    assert.equal(pool:clear(function(conninfo, conn)
        assert.is_string(conninfo)
        assert.match(conn, '^postgres.connection: ', false)
        n = n + 1
        return n < 3
    end), 2)
    assert(pool:get())
    assert.is_nil(pool:get())
end

