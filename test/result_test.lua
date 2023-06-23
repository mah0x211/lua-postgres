require('luacov')
local testcase = require('testcase')
local new_connection = require('postgres.connection').new

function testcase.status()
    local c = assert(new_connection())
    local res = assert(c:query([[
        CREATE TEMP TABLE test_tbl (
            id serial,
            str varchar,
            num integer
        )
    ]]))

    -- test that get status
    assert.equal({
        res:status(),
    }, {
        'command_ok',
    })
end

function testcase.next()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (2, 20)
        ) t1 (a, b);
        SELECT * FROM (
            VALUES (10, 100), (20, 200)
        ) t1 (a, b);
    ]]))
    assert.equal(res:value(1, 1), '1')
    assert.equal(res:value(1, 2), '10')
    assert.equal(res:value(2, 1), '2')
    assert.equal(res:value(2, 2), '20')

    -- test that get next result
    res = assert(res:next())
    assert.equal(res:value(1, 1), '10')
    assert.equal(res:value(1, 2), '100')
    assert.equal(res:value(2, 1), '20')
    assert.equal(res:value(2, 2), '200')
    -- test that return nil
    assert.is_nil(res:next())
end

function testcase.clear()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (2, 20)
        ) t1 (a, b);
        SELECT * FROM (
            VALUES (10, 100), (20, 200)
        ) t1 (a, b);
    ]]))

    -- test that clear can be called any times
    res:clear()
    res:clear()

    -- test that cannot be used after clearing
    local err = assert.throws(res.value, res, 1, 1)
    assert.match(err, 'attempt to use a freed object')
end

function testcase.close()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (2, 20)
        ) t1 (a, b);
        SELECT * FROM (
            VALUES (10, 100), (20, 200)
        ) t1 (a, b);
    ]]))

    -- test that close method clears all results
    local ok, err, timeout = res:close()
    assert.is_true(ok)
    assert.is_nil(err)
    assert.is_nil(timeout)

    -- test that connection can be reused after close
    res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (2, 20)
        ) t1 (a, b);
        SELECT * FROM (
            VALUES (10, 100), (20, 200)
        ) t1 (a, b);
    ]]))
    assert.equal(res:value(1, 1), '1')
    assert.equal(res:value(1, 2), '10')
    assert.equal(res:value(2, 1), '2')
    assert.equal(res:value(2, 2), '20')
end

function testcase.stat()
    local c = assert(new_connection())
    local res = assert(c:query([[
        CREATE TEMP TABLE test_tbl (
            id serial,
            str varchar,
            num integer
        )
    ]]))

    -- test that get stat
    assert.is_table(res:stat())
end

function testcase.value()
    local c = assert(new_connection())

    -- test that get value
    local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (NULL, 20)
        ) t1 (a, b)
    ]]))
    assert.equal(res:value(1, 1), '1')
    assert.equal(res:value(1, 2), '10')
    assert.is_nil(res:value(2, 1))
    assert.equal(res:value(2, 2), '20')
end

function testcase.rowinfo()
    local c = assert(new_connection())

    -- test that get value
    local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (NULL, 20)
        ) t1 (a, b)
    ]]))
    assert.equal({
        res:rowinfo(),
    }, {
        'tuples_ok',
        2,
    })
end

function testcase.rows()
    local c = assert(new_connection())

    -- test that get postgres.rows
    local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (NULL, 20)
        ) t1 (a, b)
    ]]))
    local rows = res:rows()
    assert.match(rows, '^postgres.rows: ', false)
    res:close()

    -- test that return nil after closed
    assert.is_nil(res:rows())

    -- test that get postgres.rows.single
    res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (NULL, 20)
        ) t1 (a, b)
    ]], nil, nil, true))
    rows = res:rows()
    assert.match(rows, '^postgres.rows.single: ', false)
    res:close()

    -- test that return error
    res = assert(c:query([[
            SELECT * FROM unknown_table
        ]]))
    local err
    rows, err = res:rows()
    assert.is_nil(rows)
    assert.match(err, 'unknown_table')
end
