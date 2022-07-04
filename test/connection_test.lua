require('luacov')
local testcase = require('testcase')
local new_connection = require('postgres.connection').new

function testcase.new()
    -- test that create new connection
    local c = assert(new_connection())
    assert.match(c, '^postgres.connection: ', false)
end

function testcase.query()
    local c = assert(new_connection())

    -- test that send query and get result
    local res, err, timeout = c:query([[
        CREATE TEMP TABLE test_tbl (
            id serial,
            str varchar,
            num integer
        )
    ]])
    assert.match(res, '^postgres.result: ', false)
    assert.is_nil(err)
    assert.is_nil(timeout)
end

function testcase.get_result()
    local c = assert(new_connection())

    -- test that send query and get result
    assert(c:query([[
        CREATE TEMP TABLE test_tbl (
            id serial,
            str varchar,
            num integer
        );
        SELECT * FROM test_tbl;
    ]]))

    local res, err, timeout = c:get_result()
    assert.match(res, '^postgres.result: ', false)
    assert.is_nil(err)
    assert.is_nil(timeout)
end

