local testcase = require('testcase')
local pgconn = require('postgres.pgconn')
local util = require('postgres.util')

function testcase.get_result_stat_for_command_ok()
    local c = assert(pgconn())
    local res = assert(c:exec([[
        CREATE TEMP TABLE copy_test (
            id serial,
            str varchar,
            num integer
        )
    ]]))
    assert.equal(res:status(), 'command_ok')

    -- test that get a result stat
    local stat = assert(util.get_result_stat(res))
    assert.equal(stat, {
        cmd_status = 'CREATE TABLE',
        oid_value = 0,
        status = 'command_ok',
    })
end

function testcase.get_result_stat_for_tuples()
    local c = assert(pgconn())
    local res = assert(c:exec_params([[
        SELECT $1 + 2 + $2 AS sum;
    ]], 1, 3))
    assert.equal(res:status(), 'tuples_ok')

    -- test that get a result stat
    local stat = assert(util.get_result_stat(res))
    assert.equal(stat.cmd_status, 'SELECT 1')
    assert.equal(stat.cmd_tuples, 1)
    assert.equal(stat.nfields, 1)
    assert.equal(stat.ntuples, 1)
    assert.is_table(stat.fields)
    assert.equal(stat.fields[1], stat.fields['sum'])
end

function testcase.get_result_stat_for_empty_query()
    local c = assert(pgconn())
    local res = assert(c:make_empty_result('empty_query'))
    assert.equal(res:status(), 'empty_query')

    -- test that get a result stat
    local stat = assert(util.get_result_stat(res))
    assert.equal(stat, {
        cmd_status = '',
        status = 'empty_query',
    })
end

function testcase.get_result_rows()
    local c = assert(pgconn())
    local res = assert(c:exec([[
        SELECT 'foo' AS col1, 1 AS col2
        UNION ALL SELECT 'bar', 2
        UNION ALL SELECT 'baz', 3;
    ]]))
    assert.equal(res:status(), 'tuples_ok')

    -- test that get all rows
    local rows = assert(util.get_result_rows(res))
    assert.equal(rows, {
        {
            'foo',
            '1',
        },
        {
            'bar',
            '2',
        },
        {
            'baz',
            '3',
        },
    })
end

function testcase.iterate_result_rows()
    local c = assert(pgconn())
    local res = assert(c:exec([[
        SELECT 'foo' AS col1, 1 AS col2
        UNION ALL SELECT 'bar', 2
        UNION ALL SELECT 'baz', 3;
    ]]))
    assert.equal(res:status(), 'tuples_ok')

    -- test that get the iterator
    local get_next, target, row = assert(util.iterate_result_rows(res, 2))
    assert.is_function(get_next)
    assert.equal(target, res)
    assert.equal(row, 2)

    -- test that iterate returns a column values
    local cols
    row, cols = get_next(target, row)
    assert.equal(row, 3)
    assert.equal(cols, {
        'baz',
        '3',
    })

    -- test that iterate returns nil when the end of rows
    row, cols = get_next(target, row)
    assert.is_nil(row)
    assert.is_nil(cols)

    -- test that iterate with for-in pairs
    local rows = {}
    -- luacheck: ignore row cols
    for row, cols in util.iterate_result_rows(res) do
        rows[row] = cols
    end
    assert.equal(rows, {
        {
            'foo',
            '1',
        },
        {
            'bar',
            '2',
        },
        {
            'baz',
            '3',
        },
    })
end
