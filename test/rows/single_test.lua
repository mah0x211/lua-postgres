require('luacov')
local testcase = require('testcase')
local new_connection = require('postgres.connection').new

function testcase.close()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT 1 AS a, 2 AS b, TRUE AS c
    ]], nil, nil, true))
    local rows = assert(res:rows())

    -- test that close result
    local ok, err, timeout = rows:close()
    assert.is_true(ok)
    assert.is_nil(err)
    assert.is_nil(timeout)
end

function testcase.result()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT 1
    ]], nil, nil, true))
    local rows = assert(res:rows())

    -- test that return result
    assert.equal(rows:result(), res)
end

function testcase.read_next()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (2, 20)
        ) t1 (a, b);
        SELECT * FROM (
            VALUES (10, 100), (20, 200)
        ) t1 (a, b);
    ]], nil, nil, true))
    local rows = assert(res:rows())
    assert(rows:next())

    -- test that read column value
    local list = {}
    local field, v = rows:read()
    while field do
        -- test that read column value
        list[#list + 1] = {
            col = #list + 1,
            name = field.name,
            value = v,
        }
        field, v = rows:read()
    end
    assert.equal(list, {
        {
            col = 1,
            name = 'a',
            value = '1',
        },
        {
            col = 2,
            name = 'b',
            value = '10',
        },
    })

    -- test that return true if next row exists
    assert.is_true(rows:next())
    list = {}
    field, v = rows:read()
    while field do
        -- test that read column value
        list[#list + 1] = {
            col = #list + 1,
            name = field.name,
            value = v,
        }
        field, v = rows:read()
    end
    assert.equal(list, {
        {
            col = 1,
            name = 'a',
            value = '2',
        },
        {
            col = 2,
            name = 'b',
            value = '20',
        },
    })

    -- test that return false if no more row exists
    assert.is_false(rows:next())

    -- test that a next method can be called twice
    assert.is_false(rows:next())

    -- test that next query result
    rows = assert(rows:result():next():rows())
    assert(rows:next())
    for i, col in pairs({
        {
            name = 'a',
            value = '10',
        },
        {
            name = 'b',
            value = '100',
        },
    }) do
        -- test that read column value
        field, v = rows:readat(i)
        assert.equal(v, col.value)
        assert.equal(field.col, i)
        assert.equal(field.name, col.name)
    end

    -- test that throws an error if msec argument is invalid
    local err = assert.throws(rows.next, rows, 'invalid msec')
    assert.match(err, 'msec must be uint')
end

