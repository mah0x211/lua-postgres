require('luacov')
local testcase = require('testcase')
local assert = require('assert')
local new_pool = require('postgres.pool').new

function testcase.new()
    -- test that create new pool initialized with default values
    local pool = new_pool()
    assert.match(pool, '^postgres%.pool: ', false)
    assert.equal(pool.maxconn, 0)
    assert.equal(pool.maxidle, 0)
    assert.equal(pool.chkintvl, 30)

    -- test that maxidle size will be set to maxconn / 3 if maxidle is nil
    pool = new_pool(9)
    assert.equal(pool.maxconn, 9)
    assert.equal(pool.maxidle, 3)

    -- test that throws an error if maxconn argument is not number
    local err = assert.throws(new_pool, 0 / 0)
    assert.match(err, 'maxconn must be number or nil')

    -- test that throws an error if maxidle argument is not number
    err = assert.throws(new_pool, nil, 0 / 0)
    assert.match(err,
                 'maxidle must be nil or number less than or equal to maxconn')

    -- test that throws an error if maxidle argument is greater than maxconn
    err = assert.throws(new_pool, 1, 2)
    assert.match(err,
                 'maxidle must be nil or number less than or equal to maxconn')

    -- test that throws an error if chkintvl argument is not number
    err = assert.throws(new_pool, nil, nil, 0 / 0)
    assert.match(err, 'chkintvl must be positive number or nil')
end

function testcase.get()
    local pool = assert(new_pool(2, 2))

    -- test that create new connection if idle connection is not exists
    local conn, err, again = pool:get()
    assert.is_nil(err)
    assert.is_nil(again)
    assert.match(conn, '^postgres%.pool%.connection: ', false)
    -- confirm that pool size is 1
    assert.equal(pool:size(), 1)
    -- confirm that used size is 1
    assert.equal(pool:size_used(), 1)
    -- confirm that idle size is 0
    assert.equal(pool:size_idle(), 0)

    -- test that create new connection if pool is empty
    local conn2
    conn2, err, again = pool:get()
    assert.is_nil(err)
    assert.is_nil(again)
    assert.match(conn, '^postgres%.pool%.connection: ', false)
    assert.not_equal(conn2, conn)
    -- confirm that used size is 2
    assert.equal(pool:size_used(), 2)
    -- confirm that idle size is 0
    assert.equal(pool:size_idle(), 0)

    -- test that return nil if reach maxconn
    local conn3
    conn3, err, again = pool:get()
    assert.is_nil(conn3)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that helth checking before return idle connection
    assert(pool:release(conn2))
    assert(pool:release(conn))
    assert.equal(pool:size(), 2)
    assert.equal(pool:size_used(), 0)
    assert.equal(pool:size_idle(), 2)
    assert(conn:close())
    conn, err, again = pool:get()
    assert.equal(conn, conn2)
    assert.is_nil(err)
    assert.is_nil(again)
    assert.equal(pool:size(), 1)
    assert.equal(pool:size_used(), 1)
    assert.equal(pool:size_idle(), 0)

    -- test that return error if pool is shutdown or closed
    pool:close()
    conn3, err, again = pool:get()
    assert.is_nil(conn3)
    assert.match(err, 'from closed or shutdown pool')
    assert.is_nil(again)
end

function testcase.release()
    local pool = assert(new_pool(2, 1))
    local conn1 = assert(pool:get())
    local conn2 = assert(pool:get())

    -- test that release connection to the idle queue
    assert(pool:release(conn1))
    assert.equal(pool:size(), 2)
    assert.equal(pool:size_used(), 1)
    assert.equal(pool:size_idle(), 1)

    -- test that use idle connection if idle connection is exists
    local conn = assert(pool:get())
    assert.equal(conn, conn1)
    assert.equal(pool:size(), 2)
    assert.equal(pool:size_used(), 2)
    assert.equal(pool:size_idle(), 0)

    -- test that connection is released if idle queue is full
    assert(pool:release(conn))
    assert(pool:release(conn2))
    assert.equal(pool:size(), 1)
    assert.equal(pool:size_used(), 0)
    assert.equal(pool:size_idle(), 1)
    assert.is_false(conn:is_connected())

    -- test that connection is closed if destroy argument is true
    conn = assert(pool:get())
    assert(pool:release(conn, true))
    assert.equal(pool:size(), 0)
    assert.equal(pool:size_used(), 0)
    assert.equal(pool:size_idle(), 0)
    assert.is_false(conn:is_connected())

    -- test that return error if failed to wait ready for query
    conn = assert(pool:get())
    assert(conn:close())
    local ok, err, timeout = pool:release(conn)
    assert.is_false(ok)
    assert.match(err, 'connection is closed')
    assert.is_nil(timeout)

    -- test that throws an error if connection is not managed by this pool
    err = assert.throws(pool.release, pool, conn2)
    assert.match(err, 'connection is not managed by this pool')

    -- test that just close connection if pool is closed
    conn = assert(pool:get())
    pool:close()
    assert(pool:release(conn))
    assert.equal(pool:size(), 0)
    assert.equal(pool:size_used(), 0)
    assert.equal(pool:size_idle(), 0)
    assert.is_false(conn:is_connected())
end

function testcase.evict()
    local pool = assert(new_pool(0, 3, 0))
    local conn1 = assert(pool:get())
    local conn2 = assert(pool:get())
    local conn3 = assert(pool:get())
    assert(pool:release(conn1))
    assert(pool:release(conn2))
    assert(pool:release(conn3))
    assert.equal(pool:size(), 3)
    assert.equal(pool:size_used(), 0)
    assert.equal(pool:size_idle(), 3)

    -- test that not evict the connection if it is still alive
    assert.equal(pool:evict(), 0)
    assert.equal(pool:size(), 3)
    assert.equal(pool:size_used(), 0)
    assert.equal(pool:size_idle(), 3)

    -- test that evict idle connection if it is not alive
    assert(conn1:close())
    assert.equal(pool:evict(), 1)
    assert.equal(pool:size(), 2)
    assert.equal(pool:size_used(), 0)
    assert.equal(pool:size_idle(), 2)

    -- test that return timeout=true if reaches timeout
    local n, timeout = pool:evict(0.0001)
    assert.equal(n, 0)
    assert.is_true(timeout)
end
