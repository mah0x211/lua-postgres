require('luacov')
local concat = table.concat
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
-- local encode = require('postgres.message').encode.command_complete
local decode = require('postgres.message').decode.command_complete

function testcase.decode()
    -- test that decode CommandComplete message
    local s = 'SELECT 1\0'
    s = 'C' .. htonl(4 + #s) .. s
    local msg, err, again = decode(s)
    assert.match(msg, '^postgres%.message%.command_complete: ', false)
    assert.contains(msg, {
        consumed = #s,
        type = 'CommandComplete',
        tag = 'SELECT',
        rows = nil,
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
    assert.match(err, 'invalid CommandComplete message')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 5
    msg, err, again = decode(concat {
        'C',
    })
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if length is not greater than 4
    msg, err, again = decode(concat {
        'C',
        htonl(1),
    })
    assert.is_nil(msg)
    assert.match(err,
                 'invalid CommandComplete message: length is not greater than 4')
    assert.is_nil(again)

    -- test that return error if message length is insufficient to unpack tag
    msg, err, again = decode(concat {
        'C',
        htonl(4 + 3),
        'SELECT\0',
    })
    assert.is_nil(msg)
    assert.match(err, 'insufficient to unpack the string')
    assert.is_nil(again)

    -- test that return error if tag is empty
    msg, err, again = decode(concat {
        'C',
        htonl(4 + 1),
        '\0',
    })
    assert.is_nil(msg)
    assert.match(err, 'empty tag')
    assert.is_nil(again)
end

-- function testcase.encode_decode()
--     -- test that encode CloseComplete message
--     local s = encode()
--     assert.equal(s, concat {
--         '3',
--         htonl(4),
--     })
-- end

