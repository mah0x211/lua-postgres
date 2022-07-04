require('luacov')
local testcase = require('testcase')
local new_connection = require('postgres.connection').new

function testcase.close()
    local c = assert(new_connection())
    local res = assert(c:query([[
        SELECT * FROM (
            VALUES (1, 10), (2, 20)
        ) t1 (a, b);
    ]]))
    local rows = assert(res:rows())
    assert(rows.res ~= nil, 'rows.res is nil')

    -- test that res will be nil after close
    rows:close()
    assert.is_nil(rows.res)
end

function testcase.next()
    local c = assert(new_connection())

    for _, single_mode in ipairs({
        false,
        true,
    }) do
        local res = assert(c:query([[
            SELECT * FROM (
                VALUES (1, 10), (2, 20)
            ) t1 (a, b);
        ]], nil, nil, single_mode))
        local rows = assert(res:rows())
        if single_mode then
            assert.match(rows, '^postgres.rows.single: ', false)
        else
            assert.match(rows, '^postgres.rows: ', false)
        end

        -- test that returns true if there is unconsumed row
        assert.is_true(rows:next())
        assert.is_false(rows:next())
        assert.is_nil(rows.res)
    end
end

function testcase.next_rows()
    local c = assert(new_connection())

    for _, single_mode in ipairs({
        false,
        true,
    }) do
        local res = assert(c:query([[
            SELECT * FROM (
                VALUES (1, 10), (2, 20)
            ) t1 (a, b);
            SELECT * FROM (
                VALUES (10, 100), (20, 200)
            ) t1 (a, b);
        ]], nil, nil, single_mode))
        local rows = assert(res:rows())
        if single_mode then
            assert.match(rows, '^postgres.rows.single: ', false)
        else
            assert.match(rows, '^postgres.rows: ', false)
        end

        assert.is_true(rows:next())
        assert.is_false(rows:next())

        -- test that get the result rows of the second query
        rows = assert(rows:next_rows())
        assert.is_true(rows:next())
        assert.is_false(rows:next())
        assert.is_nil(rows:next_rows())
        assert.is_nil(rows.res)
    end
end

function testcase.get()
    local c = assert(new_connection())

    for _, single_mode in ipairs({
        false,
        true,
    }) do
        local res = assert(c:query([[
            SELECT * FROM (
                VALUES (1, 10), (2, 20)
            ) t1 (a, b);
            SELECT * FROM (
                VALUES (10, 100), (20, 200)
            ) t1 (a, b);
        ]], nil, nil, single_mode))
        local rows = assert(res:rows())
        if single_mode then
            assert.match(rows, '^postgres.rows.single: ', false)
        else
            assert.match(rows, '^postgres.rows: ', false)
        end

        -- test that get first result rows
        local data = {}
        repeat
            local row = assert(rows:get())
            data[#data + 1] = row
        until rows:next() == false
        assert.equal(data, {
            {
                '1',
                '10',
            },
            {
                '2',
                '20',
            },
        })

        -- test that get second result rows
        rows = assert(rows:next_rows())
        data = {}
        repeat
            local row = assert(rows:get())
            data[#data + 1] = row
        until rows:next() == false
        assert.equal(data, {
            {
                '10',
                '100',
            },
            {
                '20',
                '200',
            },
        })
    end
end

function testcase.get_with_decoder()
    local c = assert(new_connection())

    for _, single_mode in ipairs({
        false,
        true,
    }) do
        local res = assert(c:query([[
            SELECT * FROM (
                VALUES (1, 10), (2, 20)
            ) t1 (a, b)
        ]], nil, nil, single_mode))
        local rows = assert(res:rows())
        if single_mode then
            assert.match(rows, '^postgres.rows.single: ', false)
        else
            assert.match(rows, '^postgres.rows: ', false)
        end

        -- test that decode values by decoder function
        local data = {}
        repeat
            local row = assert(rows:get(function(v, field)
                return tonumber(v)
            end))
            data[#data + 1] = row
        until rows:next() == false
        assert.equal(data, {
            {
                1,
                10,
            },
            {
                2,
                20,
            },
        })

        -- test that return error of decoder function
        res = assert(c:query([[
            SELECT * FROM (
                VALUES (1, 10)
            ) t1 (a, b)
        ]], nil, nil, single_mode))
        rows = assert(res:rows())
        repeat
            local row, err = rows:get(function()
                return nil, 'error from decoder'
            end)
            assert.is_nil(row)
            assert.equal(err, 'error from decoder')
        until rows:next() == false
    end
end

