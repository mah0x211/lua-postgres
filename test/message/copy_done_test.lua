require('luacov')
local concat = table.concat
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
-- Use message.lua to avoid module registration conflicts
local encode = require('postgres.message').encode.copy_done
local decode = require('postgres.message').decode.copy_done

function testcase.encode()
    -- test encode CopyDone message (always 5 bytes: 'c' + 4-byte length)
    assert.equal(encode(), concat {
        'c',
        htonl(4),
    })
end

function testcase.decode()
    -- test that decode successful message with extra data in buffer
    local msg, err, again = decode(concat {
        'c',
        htonl(4),
        'EXTRA',
    })
    assert.not_nil(msg)
    assert.is_nil(err)
    assert.is_nil(again)
    assert.equal(msg.type, 'CopyDone')
    assert.equal(msg.consumed, 5) -- should only consume the CopyDone part

    -- test that return again=true if message length is less than 5
    msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    msg, err, again = decode('c')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message type is not 'c'
    msg, err, again = decode('hello')
    assert.is_nil(msg)
    assert.match(err, 'invalid CopyDone message')
    assert.is_nil(again)

    -- test that return error if message length is not 4
    msg, err, again = decode(concat {
        'c',
        htonl(5),
        'x', -- extra data to meet length requirement
    })
    assert.is_nil(msg)
    assert.match(err, 'invalid CopyDone message: unexpected length 5')
    assert.is_nil(again)
end
