require('luacov')
local concat = table.concat
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local htons = require('postgres.htons')
local encode = require('postgres.message').encode.bind
local decode = require('postgres.message').decode.bind

function testcase.decode()
    -- test that decode Bind message
    local s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(3), -- number of parameter format codes
        htons(0), -- text
        htons(1), -- binary
        htons(0), -- text
        htons(4), -- number of parameter values
        htonl(5), -- length of value#1
        'hello', -- value#1
        htonl(0), -- length of value#2
        htonl(-1), -- length of value#3
        htonl(5), -- length of value#4
        'world', -- value#4
        htons(2), -- number of result format codes
        htons(0), -- text
        htons(1), -- binary
    })
    s = 'B' .. htonl(#s + 4) .. s
    local msg, err, again = decode(s .. 'foobarbaz')
    assert.is_nil(err)
    assert.is_nil(again)
    assert.contains(msg, {
        consumed = #s,
        type = 'Bind',
        portal = 'foo',
        stmt = 'bar',
        formats = {
            'text',
            'binary',
            'text',
        },
        values = {
            'hello',
            '',
            '\0',
            'world',
        },
        results = {
            'text',
            'binary',
        },
    })

    -- test that return again=true if message length is less than 1
    msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message is not Bind message
    msg, err, again = decode('1')
    assert.is_nil(msg)
    assert.match(err, 'invalid Bind message')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 5
    msg, err, again = decode('B')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return again=true if message length is less than 12
    msg, err, again = decode('B' .. htonl(12))
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message length is invalid
    msg, err, again = decode('B' .. htonl(0))
    assert.is_nil(msg)
    assert.match(err,
                 'message length must be greater than or equal to its own length')
    assert.is_nil(again)

    -- test that return error if message length value is less than 12
    msg, err, again = decode('B' .. htonl(11))
    assert.is_nil(msg)
    assert.match(err, 'message length must be greater than or equal to 12')
    assert.is_nil(again)

    -- test that return error if number of parameter format codes is invalid
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(-1), -- number of parameter format codes
        htons(0), -- number of parameter values
        htons(0), -- number of result format codes
    })
    msg, err, again = decode('B' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err,
                 'number of parameter format codes must be greater than or equal to 0')
    assert.is_nil(again)

    -- test that return error if parameter format code is invalid
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(1), -- number of parameter format codes
        htons(-1), -- parameter format code
        htons(0), -- number of parameter values
        htons(0), -- number of result format codes
    })
    msg, err, again = decode('B' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err, 'parameter format#1 code -1 is not supported')
    assert.is_nil(again)

    -- test that return error if message length is not enough to decode number of parameter values
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(0), -- number of parameter format codes
    })
    msg, err, again = decode('B' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err, 'not enough to decode number of parameter values')
    assert.is_nil(again)

    -- test that return error if number of parameter values is invalid
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(0), -- number of parameter format codes
        htons(-1), -- number of parameter values
        htons(0), -- number of result format codes
    })
    msg, err, again = decode('B' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err,
                 'number of parameter values must be greater than or equal to 0')
    assert.is_nil(again)

    -- test that return error if parameter value length exceeds message length
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(0), -- number of parameter format codes
        htons(2), -- number of parameter values
        htonl(5), -- length of value#1
        'hello', -- value#1
        htonl(123), -- length of value#2
        'world', -- value#2
        htons(0), -- number of result format codes
    })
    msg, err, again = decode('B' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err, 'not enough to decode parameter values')
    assert.is_nil(again)

    -- test that return error if parameter value length is invalid
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(0), -- number of parameter format codes
        htons(1), -- number of parameter values
        htonl(-2), -- length of value#1
        htons(0), -- number of result format codes
    })
    msg, err, again = decode('B' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err, 'parameter value#1 length -2 is not supported')
    assert.is_nil(again)

    -- test that return error if message length is not enough to decode number of result-column format codes
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(0), -- number of parameter format codes
        htons(0), -- number of parameter values
    })
    msg, err, again = decode('B' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err,
                 'not enough to decode number of result-column format codes')
    assert.is_nil(again)

    -- test that return error if number of result format codes is invalid
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(0), -- number of parameter format codes
        htons(0), -- number of parameter values
        htons(-1), -- number of result format codes
    })
    msg, err, again = decode('B' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err,
                 'number of result-column format codes must be greater than or equal to 0')
    assert.is_nil(again)

    -- test that return error if result-column format codes exceeds message length
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(0), -- number of parameter format codes
        htons(0), -- number of parameter values
        htons(5), -- number of result format codes
    })
    msg, err, again = decode('B' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err, 'not enough to decode result-column format codes')
    assert.is_nil(again)

    -- test that return error if result-column format code is invalid
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(0), -- number of parameter format codes
        htons(0), -- number of parameter values
        htons(1), -- number of result format codes
        htons(-1), -- result-column format code
    })
    msg, err, again = decode('B' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err, 'result-column format#1 code -1 is not supported')
    assert.is_nil(again)

    -- test that return error if message length is too long
    s = concat({
        'foo\0', -- portal
        'bar\0', -- statement
        htons(0), -- number of parameter format codes
        htons(0), -- number of parameter values
        htons(0), -- number of result format codes
    })
    msg, err, again = decode('B' .. htonl(#s + 4 + 3) .. s .. 'foo')
    assert.is_nil(msg)
    assert.match(err, 'message length is too long')
    assert.is_nil(again)
end

function testcase.encode_decode()
    -- test that encode Bind  message
    local s = encode('foo', 'bar', {
        'hello',
        'world',
    })
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.bind')
    assert.contains(msg, {
        consumed = #s,
        type = 'Bind',
        portal = 'foo',
        stmt = 'bar',
        formats = {},
        values = {
            'hello',
            'world',
        },
        results = {},
    })

    -- test that throw error if values is not string[]
    local err = assert.throws(encode, 'foo', 'bar', {
        'hello',
        123,
    })
    assert.match(err, 'values#2 must be string')
end

