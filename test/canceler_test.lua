require('luacov')
local testcase = require('testcase')
local assert = require('assert')
local encode_message = require('postgres.message').encode
local new_connection = require('postgres.connection').new
local parse_conninfo = require('postgres.conninfo')
local new_canceler = require('postgres.canceler').new

function testcase.new()
    -- test that create new cancel object
    local conninfo = select(3, assert(parse_conninfo('')))
    local canceler, err = new_canceler(conninfo, 0, 0)
    assert.match(canceler, '^postgres%.canceler: ', false)
    assert.is_nil(err)

    -- test that return err if conninfo is invalid
    canceler, err = new_canceler('invalid conninfo', 0, 0)
    assert.is_nil(canceler)
    assert.match(err, 'invalid connection string')

    -- test that cannot connect to unknown host and port
    canceler = assert(new_canceler('postgres://127.0.0.1:1234', 0, 0))
    local ok, timeout
    ok, err, timeout = canceler:cancel()
    assert.is_false(ok)
    assert.match(err, 'ECONNREFUSED')
    assert.is_nil(timeout)
end

function testcase.cancel()
    local c = assert(new_connection())
    local canceler = assert(c:get_cancel())

    -- test that send cancel request
    local ok, err, timeout = c:send(encode_message.query('SELECT pg_sleep(4)'))
    assert.is_true(ok)
    assert.is_nil(err)
    assert.is_nil(timeout)
    -- cancel running query
    ok, err, timeout = canceler:cancel()
    assert.is_true(ok)
    assert.is_nil(err)
    assert.is_nil(timeout)
    -- confirm that query is canceled
    local msg
    msg, err, timeout = c:next()
    assert.match(msg, 'postgres%.message%.row_description: ', false)
    assert.is_nil(err)
    assert.is_nil(timeout)
    local rows = assert(msg:rows())
    ok, err, timeout = rows:next()
    assert.is_false(ok)
    assert.match(err, 'cancel.* user request', false)
    assert.is_nil(timeout)
end
