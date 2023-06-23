local testcase = require('testcase')
local pgconn = require('postgres.pgconn')

function testcase.clear()
    local c = assert(pgconn())
    local res = assert(c:exec([[
        CREATE TEMP TABLE copy_test (
            id serial,
            str varchar,
            num integer
        )
    ]]))
    assert.equal(res:status(), 'command_ok')

    -- test that clear result
    res:clear()
    -- cannot call a method after clear
    local err = assert.throws(res.status, res)
    assert.match(err, 'attempt to use a freed object')

    -- test that clear method can be called more than once
    res:clear()
end

function testcase.connection()
    local c = assert(pgconn())
    local res = assert(c:exec(''))
    assert.equal(res:status(), 'empty_query')

    -- test that return a connection
    assert.equal(res:connection(), c)

    -- test that return a nil if result is cleared
    res:clear()
    assert.is_nil(res:connection())
end

function testcase.verbose_error_message()
    local c = assert(pgconn())

    -- test that get a error message
    local res = assert(c:exec([[
        SELECT * FROM example_table
    ]]))
    assert.is_string(res:verbose_error_message())
end

function testcase.error_message()
    local c = assert(pgconn())

    -- test that get a error message
    local res = assert(c:exec([[
        SELECT * FROM example_table
    ]]))
    assert.is_string(res:error_message())
end

function testcase.error_field()
    local c = assert(pgconn())

    -- test that get a error message field
    local res = assert(c:exec([[
        SELECT * FROM example_table
    ]]))
    for _, field in ipairs({
        'severity',
        'severity_nonlocalize',
        'sqlstate',
        'message_primary',
        'message_detail',
        'message_hint',
        'statement_position',
        'internal_position',
        'internal_query',
        'context',
        'schema_name',
        'table_name',
        'column_name',
        'datatype_name',
        'constraint_name',
        'source_file',
        'source_line',
        'source_function',
    }) do
        local v = res:error_field(field)
        if v then
            assert.is_string(v)
        end
    end
end

function testcase.get_attributes()
    local c = assert(pgconn())
    local res = assert(c:exec([[
        CREATE TEMP TABLE copy_test (
            id serial,
            str varchar,
            num integer
        )
    ]]))
    assert.equal(res:status(), 'command_ok')

    res = assert(c:exec([[
        INSERT INTO copy_test (str, num)
        VALUES (
            'foo', 123
        ) RETURNING id
    ]]))

    assert.equal(res:status(), 'tuples_ok')
    assert.is_nil(res:error_message())
    assert.match(res:verbose_error_message(), 'PGresult is not an error result')
    assert.equal(res:ntuples(), 1)
    assert.equal(res:nfields(), 1)
    assert.is_false(res:binary_tuples())
    assert.equal(res:fname(1), 'id')
    assert.equal(res:fnumber('id'), 1)
    assert.is_int(res:ftable(1))
    assert.equal(res:ftablecol(1), 1)
    assert.equal(res:fformat(1), 'text')
    assert.is_int(res:ftype(1))
    assert.greater(res:fsize(1), 0)
    assert.equal(res:fmod(1), -1)
    assert.equal(res:cmd_status(), 'INSERT 0 1')
    assert.is_nil(res:oid_value(1))
    assert.equal(res:cmd_tuples(), 1)
    assert.equal(res:nparams(), 0)
end

function testcase.get_value()
    local c = assert(pgconn())
    local res = assert(c:exec([[
        CREATE TEMP TABLE test_tbl (
            id serial,
            str varchar,
            num integer
        )
    ]]))
    assert.equal(res:status(), 'command_ok')
    res = assert(c:exec([[
        INSERT INTO test_tbl (str, num)
        VALUES (
            'foo', 101
        ), (
            'bar', 102
        )
    ]]))
    assert.equal(res:status(), 'command_ok')

    -- test that get value of each row
    res = assert(c:exec([[
        SELECT * FROM test_tbl
    ]]))
    assert.equal(res:status(), 'tuples_ok')

    local rows = {}
    for row = 1, res:ntuples() do
        local line = {}
        for col = 1, res:nfields() do
            local val = res:get_value(row, col)
            if val then
                local len = res:get_length(row, col)
                assert.equal(len, #val)
                line[col] = val
            end
        end
        rows[#rows + 1] = line
    end
    assert.equal(rows, {
        {
            '1',
            'foo',
            '101',
        },
        {
            '2',
            'bar',
            '102',
        },
    })
end

function testcase.get_is_null()
    local c = assert(pgconn())

    -- test that true if a value of specified row and column is null
    local res = assert(c:exec([[
        SELECT 'foo', NULL, 123;
    ]]))
    assert.is_false(res:get_is_null(1, 1))
    assert.is_true(res:get_is_null(1, 2))
    assert.is_false(res:get_is_null(1, 3))
    assert.is_true(res:get_is_null(1, 4))
end

