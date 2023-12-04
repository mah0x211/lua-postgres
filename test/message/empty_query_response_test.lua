require('luacov')
local concat = table.concat
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local decode = require('postgres.message').decode.empty_query_response

function testcase.decode()
    -- test that decode EmptyQueryResponse message
    local msg, err, again = decode('I' .. htonl(4))
    assert.match(msg, '^postgres%.message%.empty_query_response: ', false)
    assert.contains(msg, {
        consumed = 5,
        type = 'EmptyQueryResponse',
    })
    assert.is_nil(err)
    assert.is_nil(again)

    -- test that return again=true if message length is less than 1
    msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if type is not 'I'
    msg, err, again = decode(concat {
        '1',
    })
    assert.is_nil(msg)
    assert.match(err, 'invalid EmptyQueryResponse message')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 5
    msg, err, again = decode(concat {
        'I',
    })
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if length is less than 4
    msg, err, again = decode(concat {
        'I',
        htonl(5),
    })
    assert.is_nil(msg)
    assert.match(err, 'length must be 4')
    assert.is_nil(again)
end
