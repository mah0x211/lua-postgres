require('luacov')
local testcase = require('testcase')
local new_connection = require('postgres.connection').new

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

function testcase.is_null()
    local c = assert(new_connection())

    -- test that returns whether the value is null or not
    local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (NULL, 20)
        ) t1 (a, b)
    ]]))
    assert.is_false(res:is_null(1, 2))
    assert.is_true(res:is_null(2, 1))
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
    assert.equal(res:value(2, 1), '')
    assert.equal(res:value(2, 2), '20')
end

function testcase.rows()
    local c = assert(new_connection())

    for _, single_mode in ipairs({
        false,
        true,
    }) do
        -- test that get rows
        local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (2, 20), (3, 30)
        ) t1 (a, b)
    ]], nil, nil, single_mode))
        local rows = assert(res:rows())
        if single_mode then
            assert.match(rows, '^postgres.rows.single: ', false)
        else
            assert.match(rows, '^postgres.rows: ', false)
        end
        rows:close()
        res:next()
    end
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

    -- test that get next result
    res = assert(res:next())
    assert.equal(res:value(1, 1), '10')
    assert.equal(res:value(1, 2), '100')
    assert.equal(res:value(2, 1), '20')
    assert.equal(res:value(2, 2), '200')
    -- test that return nil
    assert.is_nil(res:next())
end

