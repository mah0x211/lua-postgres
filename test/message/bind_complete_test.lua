require('luacov')
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local encode = require('postgres.message').encode.bind_complete
local decode = require('postgres.message').decode.bind_complete

function testcase.decode()
    -- test that return again=true if message length is less than 1
    local msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message is not BindComplete message
    msg, err, again = decode('1')
    assert.is_nil(msg)
    assert.match(err, 'invalid BindComplete message')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 5
    msg, err, again = decode('2')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message length is not 4
    msg, err, again = decode('2' .. htonl(3))
    assert.is_nil(msg)
    assert.match(err, 'invalid BindComplete message')
    assert.is_nil(again)
end

function testcase.encode_decode()
    -- test that encode BindComplete  message
    local s = encode()
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.bind_complete')
    assert.contains(msg, {
        consumed = #s,
        type = 'BindComplete',
    })
end

