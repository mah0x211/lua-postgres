require('luacov')
local concat = table.concat
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local htons = require('postgres.htons')

-- Use message.lua to avoid module registration conflicts
local decode = require('postgres.message').decode.copy_both_response

function testcase.decode()
    -- test that basic text format with no columns
    local msg, err, again = assert(decode(concat {
        'W',
        htonl(7), -- length: 4 + 1 + 2
        string.char(0), -- overall format: text (0)
        htons(0), -- 0 columns
    }))
    assert.equal(msg.type, 'CopyBothResponse')
    assert.equal(msg.format, 'text')
    assert.equal(msg.column_count, 0)
    assert.is_nil(err)
    assert.is_nil(again)

    -- test that binary format with 2 columns
    msg, err, again = assert(decode(concat {
        'W',
        htonl(11), -- length: 4 + 1 + 2 + 4
        string.char(1), -- overall format: binary (1)
        htons(2), -- 2 columns
        htons(1), -- column 1: binary
        htons(1), -- column 2: binary
    }))
    assert.equal(msg.type, 'CopyBothResponse')
    assert.equal(msg.format, 'binary')
    assert.equal(msg.column_count, 2)
    assert.is_nil(err)
    assert.is_nil(again)

    -- test that insufficient data for header (needs at least 8 bytes)
    msg, err, again = decode('short')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that invalid message type
    msg, err, again = decode(concat {
        'X', -- invalid type
        htonl(7),
        string.char(0),
        htons(0),
    })
    assert.is_nil(msg)
    assert.match(err, 'invalid CopyBothResponse message')
    assert.is_nil(again)

    -- test that insufficient data for complete message
    msg, err, again = decode(concat {
        'W',
        htonl(15), -- length requires more data than provided
        string.char(1),
        htons(3),
        htons(1),
        htons(1), -- incomplete data
    })
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that unsupported copy format
    msg, err, again = decode(concat {
        'W',
        htonl(7),
        string.char(2), -- invalid format (not 0 or 1)
        htons(0),
    })
    assert.is_nil(msg)
    assert.match(err, 'unsupported copy format 2')
    assert.is_nil(again)

    -- test that length mismatch
    msg, err, again = decode(concat {
        'W',
        htonl(10), -- wrong length (should be 11 for 2 columns)
        string.char(1),
        htons(2),
        htons(1),
        htons(1),
    })
    assert.is_nil(msg)
    assert.match(err, 'length mismatch, expected 11 but got 10')
    assert.is_nil(again)

    -- test that column format does not match overall format
    msg, err, again = decode(concat {
        'W',
        htonl(9),
        string.char(0), -- overall text format
        htons(1), -- 1 column
        htons(1), -- but column is binary - should be 0 for text
    })
    assert.is_nil(msg)
    assert.match(err, 'column#1 format 1 does not match overall format 0')
    assert.is_nil(again)
end
