require('luacov')
local concat = table.concat
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local encode = require('postgres.message').encode.cancel_request
local decode = require('postgres.message').decode.cancel_request

function testcase.decode()
    local msg, err, again = decode(concat {
        htonl(16),
        htonl(80877102),
        htonl(123),
        htonl(456),
    })
    assert.match(msg, '^postgres%.message%.cancel_request: ', false)
    assert.contains(msg, {
        consumed = 16,
        pid = 123,
        key = 456,
    })
    assert.is_nil(err)
    assert.is_nil(again)

    -- test that return again=true if message length is less than 4
    msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if length is less than 16
    msg, err, again = decode(concat {
        htonl(3),
    })
    assert.is_nil(msg)
    assert.match(err, 'length must be 16')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 8
    msg, err, again = decode(concat {
        htonl(16),
    })
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if code is not 80877102
    msg, err, again = decode(concat {
        htonl(16),
        htonl(1),
    })
    assert.is_nil(msg)
    assert.match(err, 'code must be 80877102')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 16
    msg, err, again = decode(concat {
        htonl(16),
        htonl(80877102),
    })
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)
end

function testcase.encode_decode()
    -- test that encode CancelRequest  message
    local s = encode(123, 456)
    assert.equal(s, concat {
        htonl(16),
        htonl(80877102),
        htonl(123),
        htonl(456),
    })
end

