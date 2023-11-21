require('luacov')
local testcase = require('testcase')
local assert = require('assert')
local new_connection = require('postgres.connection').new

function testcase.close()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT 1 AS a, 2 AS b, TRUE AS c
    ]]))

    -- test that return postgres.rows object
    local rows = assert(res:rows())
    assert.match(rows, '^postgres%.rows: ', false)

    -- test that close associated result
    local ok, err, timeout = rows:close()
    assert.is_true(ok)
    assert.is_nil(err)
    assert.is_nil(timeout)
    assert.match(rows.complete, '^postgres%.message%.command_complete: ', false)
end

function testcase.next()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT 1, 10
    ]]))
    local rows = assert(res:rows())

    -- test that return true if the first row exists
    assert.is_true(rows:next())
    -- test that return false if no more row exists
    assert.is_false(rows:next())
end

function testcase.readat()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT 123 AS a, 456 AS b
    ]]))
    local rows = assert(res:rows())
    assert(rows:next())

    -- test that read specified column value
    for i, cmp in ipairs({
        {
            name = 'a',
            value = '123',
        },
        {
            name = 'b',
            value = '456',
        },
    }) do
        local field, v = rows:readat(i)
        assert.equal(field.name, cmp.name)
        assert.equal(v, cmp.value)
    end

    -- test that return nil if specified column not exists
    local field, v = rows:readat(3)
    assert.is_nil(field)
    assert.is_nil(v)
end

function testcase.read()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT 123 AS a, 456 AS b
    ]]))
    local rows = assert(res:rows())
    assert(rows:next())

    -- test that read each column value
    for _, cmp in ipairs({
        {
            name = 'a',
            value = '123',
        },
        {
            name = 'b',
            value = '456',
        },
    }) do
        local field, v = rows:read()
        assert.equal(field.name, cmp.name)
        assert.equal(v, cmp.value)
    end

    -- test that return nil if no more column exists
    local field, v = rows:read()
    assert.is_nil(field)
    assert.is_nil(v)
end

function testcase.scanat()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT 123::integer AS a, '1999-05-12'::date AS b
    ]]))
    local rows = assert(res:rows())
    assert(rows:next())

    -- test that scan specified column value and return the decoded value
    for i, cmp in ipairs({
        {
            name = 'a',
            value = 123,
        },
        {
            name = 'b',
            value = {
                year = 1999,
                month = 5,
                day = 12,
            },
        },
    }) do
        local field, v, err = rows:scanat(i)
        assert.is_nil(err)
        assert.equal(field.name, cmp.name)
        assert.equal(v, cmp.value)
    end

    -- test that return nil if specified column not exists
    local field, v, err = rows:scanat(3)
    assert.is_nil(v)
    assert.is_nil(err)
    assert.is_nil(field)
end

function testcase.scan()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT 123::integer AS a, '1999-05-12'::date AS b
    ]]))
    local rows = assert(res:rows())
    assert(rows:next())

    -- test that scan each column value and return the decoded value
    for _, cmp in ipairs({
        {
            name = 'a',
            value = 123,
        },
        {
            name = 'b',
            value = {
                year = 1999,
                month = 5,
                day = 12,
            },
        },
    }) do
        local field, v, err = rows:scan()
        assert.is_nil(err)
        assert.equal(field.name, cmp.name)
        assert.equal(v, cmp.value)
    end

    -- test that return nil if specified column not exists
    local field, v, err = rows:scan()
    assert.is_nil(v)
    assert.is_nil(err)
    assert.is_nil(field)
end
