require('luacov')
local concat = table.concat
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local encode = require('postgres.message').encode.close
local decode = require('postgres.message').decode.close

function testcase.decode()
    -- test that decode Close message for statement
    local s = concat({
        'C',
        htonl(4 + 1 + 4),
        'S',
        'foo\0',
    })
    local msg, err, again = decode(s)
    assert.match(msg, '^postgres%.message%.close: ', false)
    assert.contains(msg, {
        consumed = #s,
        type = 'CloseStatement',
        name = 'foo',
    })
    assert.is_nil(err)
    assert.is_nil(again)

    -- test that decode Close message for portal
    s = concat({
        'C',
        htonl(4 + 1 + 4),
        'P',
        'foo\0',
    })
    msg, err, again = decode(s)
    assert.match(msg, '^postgres%.message%.close: ', false)
    assert.contains(msg, {
        consumed = #s,
        type = 'ClosePortal',
        name = 'foo',
    })
    assert.is_nil(err)
    assert.is_nil(again)

    -- test that return again=true if message length is less than 1
    msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if type is not 'C'
    msg, err, again = decode(concat {
        '1',
    })
    assert.is_nil(msg)
    assert.match(err, 'invalid Close message')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 5
    msg, err, again = decode(concat {
        'C',
    })
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if length is less than 6
    msg, err, again = decode(concat {
        'C',
        htonl(5),
    })
    assert.is_nil(msg)
    assert.match(err, 'length must be greater than 6')
    assert.is_nil(again)

    -- test that return again=true if message length is less than length
    msg, err, again = decode(concat {
        'C',
        htonl(4 + 1 + 4),
        'S',
    })
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message length is insufficient to decode name
    msg, err, again = decode(concat {
        'C',
        htonl(4 + 1 + 4),
        'S',
        'foobar',
    })
    assert.is_nil(msg)
    assert.match(err, 'insufficient to unpack the string')
    assert.is_nil(again)

    -- test that return error if target is not 'S' or 'P'
    msg, err, again = decode(concat {
        'C',
        htonl(4 + 1 + 4),
        'X',
        'foo\0',
    })
    assert.is_nil(msg)
    assert.match(err, 'target is not "S" or "P"')
    assert.is_nil(again)
end

function testcase.encode_decode()
    -- test that encode CloseStatement message
    local s = encode('statement', 'foo')
    local msg, err, again = decode(s)
    assert.match(msg, '^postgres%.message%.close: ', false)
    assert.contains(msg, {
        consumed = #s,
        type = 'CloseStatement',
        name = 'foo',
    })
    assert.is_nil(err)
    assert.is_nil(again)

    -- test that encode ClosePortal message
    s = encode('portal', 'foo')
    msg, err, again = decode(s)
    assert.match(msg, '^postgres%.message%.close: ', false)
    assert.contains(msg, {
        consumed = #s,
        type = 'ClosePortal',
        name = 'foo',
    })
    assert.is_nil(err)
    assert.is_nil(again)
end

