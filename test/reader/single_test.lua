require('luacov')
local testcase = require('testcase')
local new_connection = require('postgres.connection').new

function testcase.each()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (2, 20)
        ) t1 (a, b);
        SELECT * FROM (
            VALUES (10, 100), (20, 200)
        ) t1 (a, b);
    ]], nil, nil, true))

    -- test that read result
    local reader = assert(res:reader())
    local rows = {}
    for row, field, val in reader:each() do
        local cols = rows[row]
        if not cols then
            cols = {}
            rows[row] = cols
        end

        cols[field.name] = {
            col = field.col,
            val = val,
        }
    end
    assert.equal(rows, {
        {
            a = {
                col = 1,
                val = '1',
            },
            b = {
                col = 2,
                val = '10',
            },
        },
        {
            a = {
                col = 1,
                val = '2',
            },
            b = {
                col = 2,
                val = '20',
            },
        },
    })

    -- test that cannot read after consumed
    rows = {}
    for row, field, val in reader:each() do
        local cols = rows[row]
        if not cols then
            cols = {}
            rows[row] = cols
        end

        cols[field.name] = {
            col = field.col,
            val = val,
        }
    end
    assert.equal(rows, {})

    -- test that result of reader
    res = assert(reader:result())
    res:clear()
    res = assert(res:next())
    reader = assert(res:reader())
    rows = {}
    for row, field, val in reader:each() do
        local cols = rows[row]
        if not cols then
            cols = {}
            rows[row] = cols
        end

        cols[field.name] = {
            col = field.col,
            val = val,
        }
    end
    assert.equal(rows, {
        {
            a = {
                col = 1,
                val = '10',
            },
            b = {
                col = 2,
                val = '100',
            },
        },
        {
            a = {
                col = 1,
                val = '20',
            },
            b = {
                col = 2,
                val = '200',
            },
        },
    })
end

