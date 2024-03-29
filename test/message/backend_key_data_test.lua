require('luacov')
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local encode = require('postgres.message').encode.backend_key_data
local decode = require('postgres.message').decode.backend_key_data

function testcase.decode()
    -- test that return again=true if message length is less than 1
    local msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message is not BackendKeyData message
    msg, err, again = decode('1')
    assert.is_nil(msg)
    assert.match(err, 'invalid BackendKeyData message')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 5
    msg, err, again = decode('K')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message length is not 12
    msg, err, again = decode('K' .. htonl(13))
    assert.is_nil(msg)
    assert.match(err, 'invalid BackendKeyData message')
    assert.is_nil(again)

    -- test that return error if message length is not 12
    msg, err, again = decode('K' .. htonl(12) .. htonl(123))
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)
end

function testcase.encode_decode()
    -- test that encode BackendKeyData message
    local s = encode(123, 456)
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.backend_key_data')
    assert.contains(msg, {
        consumed = #s,
        type = 'BackendKeyData',
        pid = 123,
        key = 456,
    })

    -- test that throw error if pid is not integer
    local err = assert.throws(encode, 1.23)
    assert.match(err, 'int32_t expected,')

    -- test that throw error if key is not integer
    err = assert.throws(encode, 123, 4.56)
    assert.match(err, 'int32_t expected,')
end

