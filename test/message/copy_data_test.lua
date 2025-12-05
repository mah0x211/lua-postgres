require('luacov')
local concat = table.concat
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
-- Use message.lua to avoid module registration conflicts
local encode = require('postgres.message').encode.copy_data
local decode = require('postgres.message').decode.copy_data

function testcase.encode()
    -- test that encode empty string
    local data = ''
    assert.equal(encode(data), concat {
        'd',
        htonl(4 + #data),
        data,
    })

    -- test that encode simple string
    data = 'hello'
    assert.equal(encode(data), concat {
        'd',
        htonl(4 + #data),
        data,
    })

    -- test that encode binary data
    data = string.char(0, 1, 2, 3, 255)
    assert.equal(encode(data), concat {
        'd',
        htonl(4 + #data),
        data,
    })

    -- test that encode large data
    data = string.rep('x', 1000)
    assert.equal(encode(data), concat {
        'd',
        htonl(4 + #data),
        data,
    })
end

function testcase.decode()
    -- test that decode successful message
    local data = 'payload'
    local msg, err, again = decode(concat {
        'd',
        htonl(4 + #data),
        data,
        'EXTRA',
    })
    assert.not_nil(msg)
    assert.is_nil(err)
    assert.is_nil(again)
    assert.equal(msg.type, 'CopyData')
    assert.equal(msg.data, data)
    assert.equal(msg.consumed, 5 + #data) -- should only consume the CopyData part

    -- test that return again=true if message length is less than 5
    msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    msg, err, again = decode('d')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return again=true if message length is less than declared length
    msg, err, again = decode(concat {
        'd',
        htonl(10),
        'short',
    })
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message type is not 'd'
    msg, err, again = decode('hello')
    assert.is_nil(msg)
    assert.match(err, 'invalid CopyData message')
    assert.is_nil(again)
end

