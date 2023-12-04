require('luacov')
local concat = table.concat
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local encode = require('postgres.message').encode.close_complete
local decode = require('postgres.message').decode.close_complete

function testcase.decode()
    local msg, err, again = decode(concat {
        '3',
        htonl(4),
    })
    assert.match(msg, '^postgres%.message%.close_complete: ', false)
    assert.contains(msg, {
        consumed = 5,
        type = 'CloseComplete',
    })
    assert.is_nil(err)
    assert.is_nil(again)

    -- test that return again=true if message length is less than 1
    msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if type is not 3
    msg, err, again = decode(concat {
        '1',
    })
    assert.is_nil(msg)
    assert.match(err, 'invalid CloseComplete message')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 5
    msg, err, again = decode(concat {
        '3',
    })
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if length is not 4
    msg, err, again = decode(concat {
        '3',
        htonl(1),
    })
    assert.is_nil(msg)
    assert.match(err, 'invalid CloseComplete message: length must be 4')
    assert.is_nil(again)
end

function testcase.encode_decode()
    -- test that encode CloseComplete message
    local s = encode()
    assert.equal(s, concat {
        '3',
        htonl(4),
    })
end

