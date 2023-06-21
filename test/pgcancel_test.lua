local testcase = require('testcase')
local pgconn = require('postgres.pgconn')

function testcase.get_cancel()
    local c = assert(pgconn())

    -- test that get a cancel structure
    local cancel = assert(c:get_cancel())
    assert.match(cancel, '^postgres%.pgcancel: ', false)
end

function testcase.cancel()
    local c = assert(pgconn())

    -- test that cancel a query
    local cancel = assert(c:get_cancel())
    assert.is_true(cancel:cancel())

    -- test that return false after cancel structure is freed
    assert(cancel:free())
    assert.is_false(cancel:cancel())
end

function testcase.free()
    local c = assert(pgconn())

    -- test that return a true if cancel structure is freed
    local cancel = assert(c:get_cancel())
    assert.is_true(cancel:free())

    -- test that return a false if cancel structure is not freed
    assert.is_false(cancel:free())
end
