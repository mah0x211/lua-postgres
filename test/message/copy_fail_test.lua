require('luacov')
local concat = table.concat
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
-- Use message.lua to avoid module registration conflicts
local encode = require('postgres.message').encode.copy_fail
local decode = require('postgres.message').decode.copy_fail

function testcase.encode()
    -- test encode CopyFail message with error message
    local errmsg = 'COPY operation failed: invalid data format'
    assert.equal(encode(errmsg), concat {
        'f',
        htonl(4 + #errmsg),
        errmsg,
    })

    -- test encode CopyFail message with empty error message
    errmsg = ''
    assert.equal(encode(errmsg), concat {
        'f',
        htonl(4 + #errmsg),
        errmsg,
    })

    -- test encode CopyFail message with special characters
    errmsg = 'Error: invalid UTF-8 sequence \xff\xfe'
    assert.equal(encode(errmsg), concat {
        'f',
        htonl(4 + #errmsg),
        errmsg,
    })
end

function testcase.decode()
    -- test that decode successful message with extra data in buffer
    local errmsg = 'Test failure message'
    local msg, err, again = decode(concat {
        'f',
        htonl(4 + #errmsg),
        errmsg,
        'EXTRA',
    })
    assert.not_nil(msg)
    assert.is_nil(err)
    assert.is_nil(again)
    assert.equal(msg.type, 'CopyFail')
    assert.equal(msg.message, errmsg)
    assert.equal(msg.consumed, 5 + #errmsg) -- should only consume the CopyFail part

    -- test that return again=true if message length is less than 5
    msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    msg, err, again = decode('f')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return again=true if message length is less than declared length
    msg, err, again = decode(concat {
        'f',
        htonl(10),
        'short',
    })
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message type is not 'f'
    msg, err, again = decode('hello')
    assert.is_nil(msg)
    assert.match(err, 'invalid CopyFail message')
    assert.is_nil(again)

    -- test that decode CopyFail with empty error message
    msg, err, again = decode(concat {
        'f',
        htonl(4),
    })
    assert.not_nil(msg)
    assert.is_nil(err)
    assert.is_nil(again)
    assert.equal(msg.type, 'CopyFail')
    assert.equal(msg.message, '')
    assert.equal(msg.consumed, 5)

    -- test that decode CopyFail with unicode error message
    local unicode_err = 'エラー：データ形式が正しくありません'
    msg, err, again = decode(concat {
        'f',
        htonl(4 + #unicode_err),
        unicode_err,
    })
    assert.not_nil(msg)
    assert.is_nil(err)
    assert.is_nil(again)
    assert.equal(msg.type, 'CopyFail')
    assert.equal(msg.message, unicode_err)
    assert.equal(msg.consumed, 5 + #unicode_err)
end