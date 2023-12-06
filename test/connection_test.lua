require('luacov')
local testcase = require('testcase')
local assert = require('assert')
local new_connection = require('postgres.connection').new

function testcase.new()
    -- test that create new connection
    local c = assert(new_connection())
    assert.match(c, '^postgres%.connection: ', false)

    -- test that create new connection with sec
    c = assert(new_connection(nil, 1.0))
    assert.match(c, '^postgres%.connection: ', false)
end

function testcase.close()
    local c = assert(new_connection())

    -- test that close method can be called any times
    local ok, err, timeout = c:close()
    assert.is_true(ok)
    assert.is_nil(err)
    assert.is_nil(timeout)

    ok, err, timeout = c:close()
    assert.is_true(ok)
    assert.is_nil(err)
    assert.is_nil(timeout)
end

function testcase.get_cancel()
    local c = assert(new_connection())

    -- test that get a cancel object
    local cancel, err = c:get_cancel()
    assert.is_nil(err)
    assert.match(cancel, '^postgres%.canceler: ', false)
end

function testcase.replace_named_params()
    local c = assert(new_connection())

    -- test that replace named parameters to positional parameters
    local params = {
        'hello',
        foo = 'foo',
        bar = {
            1,
            'bar',
            {
                11,
                12,
            },
            2,
        },
        baz = 'baz',
    }
    local qry, err, newparams = c:replace_named_params(
                                    'SELECT ${foo}, ${bar}, ${baz}, ${foo}, ${bar}, ${unknown}',
                                    params)
    assert.equal(qry,
                 'SELECT $2, $3, $4, {$5, $6}, $7, $8, $2, $3, $4, {$5, $6}, $7, $9')
    assert.equal(newparams, {
        'hello',
        'foo',
        '1',
        'bar',
        '11',
        '12',
        '2',
        'baz',
        'NULL',
    })
    assert.is_nil(err)
end

function testcase.query()
    local c = assert(new_connection())

    -- test that send query with parameters and get result
    local res, err, timeout = c:query([[
        SELECT ${foo}, ${bar}, ${baz}, $1, ${foo}
    ]], {
        foo = 'foo',
        bar = 'bar',
        baz = 'baz',
        'hello',
    })
    assert.match(res, '^postgres%.message%.row_description: ', false)
    assert.is_nil(err)
    assert.is_nil(timeout)

    local rows = assert(res:rows())
    local cols = {}
    assert(rows:next())
    for _ = 1, 5 do
        local field, val = rows:read()
        cols[field.col] = val
    end
    assert.equal(cols, {
        'foo',
        'bar',
        'baz',
        'hello',
        'foo',
    })
    assert.is_false(rows:next())
    assert.match(rows.complete, '^postgres%.message%.command_complete: ', false)
end

function testcase.ping()
    local c = assert(new_connection())

    -- test that ping
    local ok, err, timeout = c:ping()
    assert.is_true(ok)
    assert.is_nil(err)
    assert.is_nil(timeout)
end

function testcase.next()
    local c = assert(new_connection())

    -- test that send multiple queries and get messages
    local msg, err, timeout = c:query([[
        CREATE TEMP TABLE test_tbl (
            id serial,
            str varchar,
            num integer
        );
        SELECT * FROM test_tbl;
    ]])
    assert.match(msg, '^postgres%.message%.command_complete: ', false)
    assert.is_nil(err)
    assert.is_nil(timeout)
    assert.equal(msg.tag, 'CREATE TABLE')

    -- test that get next message
    msg, err, timeout = c:next()
    assert.match(msg, '^postgres%.message%.row_description: ', false)
    assert.is_nil(err)
    assert.is_nil(timeout)
end

