require('luacov')
local testcase = require('testcase')
local new_connection = require('postgres.connection').new

function testcase.new()
    -- test that create new connection
    local c = assert(new_connection())
    assert.match(c, '^postgres.connection: ', false)

    -- test that create new connection with deadline
    c = assert(new_connection(nil, 1000))
    assert.match(c, '^postgres.connection: ', false)
end

function testcase.close()
    local c = assert(new_connection())

    -- test that close method can be called any times
    c:close()
    c:close()
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
    local qry, newparams, err = c:replace_named_params(
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
    assert.match(res, '^postgres.result: ', false)
    assert.is_nil(err)
    assert.is_nil(timeout)

    local rows = assert(res:rows())
    local cols = {}
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

