require('luacov')
local concat = table.concat
local sub = string.sub
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local htons = require('postgres.htons')
local decode = require('postgres.message').decode.data_row

function testcase.decode()
    -- test that decode DataRow message
    local s = concat({
        htons(4), -- number of column values
        htonl(3), -- length of value#1
        'foo', -- value#1
        htonl(0), -- length of value#2
        htonl(-1), -- length of value#3
        htonl(5), -- length of value#4
        'world', -- value#4
    })
    s = 'D' .. htonl(4 + #s) .. s
    local msg, err, again = decode(s .. 'foobarbaz')
    assert.match(msg, '^postgres%.message%.data_row: ', false)
    assert.is_nil(err)
    assert.is_nil(again)
    assert.contains(msg, {
        consumed = #s,
        type = 'DataRow',
        values = {
            'foo',
            '',
            '\0',
            'world',
        },
    })

    -- test that return again=true if message length is less than 1
    msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message is not DataRow message
    msg, err, again = decode('1')
    assert.is_nil(msg)
    assert.match(err, 'invalid DataRow message')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 5
    msg, err, again = decode('D')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if length is invalid
    msg, err, again = decode('D' .. htonl(-1))
    assert.is_nil(msg)
    assert.match(err, 'length must be greater than or equal to its own length')
    assert.is_nil(again)

    -- test that return error if length is less than 6
    msg, err, again = decode('D' .. htonl(5))
    assert.is_nil(msg)
    assert.match(err, 'length is not greater than 5')
    assert.is_nil(again)

    -- test that return again=true if message length is not enough
    s = concat({
        htons(4), -- number of column values
        htonl(3), -- length of value#1
        'foo', -- value#1
        htonl(0), -- length of value#2
        htonl(-1), -- length of value#3
        htonl(5), -- length of value#4
        'world', -- value#4
    })
    s = 'D' .. htonl(4 + #s) .. sub(s, 1, #s - 5)
    msg, err, again = decode(s)
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if number of column values is invalid
    s = concat({
        htons(-1), -- number of column values
    })
    msg, err, again = decode('D' .. htonl(#s + 4) .. s)
    assert.is_nil(msg)
    assert.match(err,
                 'number of column values is not greater than or equal to 0')
    assert.is_nil(again)

    -- test that return error if message length is not enough to decode column values
    s = concat({
        htons(1), -- number of column values
        htonl(10), -- length of value#1
        'foo', -- value#1
    })
    s = 'D' .. htonl(4 + #s) .. s
    msg, err, again = decode(s)
    assert.is_nil(msg)
    assert.match(err, 'message length is not enough to decode column values')
    assert.is_nil(again)

    -- test that return error if length of column value is invalid
    s = concat({
        htons(1), -- number of column values
        htonl(-2), -- length of value#1
    })
    s = 'D' .. htonl(4 + #s) .. s
    msg, err, again = decode(s)
    assert.is_nil(msg)
    assert.match(err, 'column value#1 length -2 is not supported')
    assert.is_nil(again)

    -- test that return error if message length is too long
    s = concat({
        htons(1), -- number of column values
        htonl(3), -- length of value#1
        'foo', -- value#1
        'foobarbaz', -- invalid data
    })
    s = 'D' .. htonl(4 + #s) .. s
    msg, err, again = decode(s)
    assert.is_nil(msg)
    assert.match(err, 'message length is too long')
    assert.is_nil(again)
end

