local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local htons = require('postgres.htons')
local unpack = require('postgres.unpack')

function testcase.unpack_i16()
    -- test that unpack int16 value
    local v = {}
    local consumed, err, again = unpack(v, 'h', htons(100))
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            100,
        },
        2,
    })

    -- test that unpack int16 for number of times specified by length modifier
    v = {}
    consumed, err, again = unpack(v, 'h3',
                                  htons(200) .. htons(201) .. htons(202))
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            200,
            201,
            202,
        },
        6,
    })

    -- test that unpack int16 for number of times specified by preceding int16
    v = {}
    consumed, err, again = unpack(v, 'hh*',
                                  htons(3) .. htons(200) .. htons(201) ..
                                      htons(202))
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            3,
            200,
            201,
            202,
        },
        8,
    })

    -- test that preceding negative int16 value treated as zero
    v = {}
    consumed, err, again = unpack(v, 'hh*', htons(-1) .. htons(200))
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            -1,
        },
        2,
    })

    -- test that return again=true if message length is less than 2 bytes
    consumed, err, again = unpack(v, 'h', '\0')
    assert.equal({
        consumed,
        err,
        again,
    }, {
        nil,
        nil,
        true,
    })

    -- test that throw error if format with '*' length modifier without preceding number format
    err = assert.throws(unpack, v, 'sh*', 'hello\0' .. htons(100))
    assert.match(err, 'must be preceded by the integer type specifier')
end

function testcase.unpack_i32()
    -- test that unpack int32 value
    local v = {}
    local consumed, err, again = unpack(v, 'i', htonl(100))
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            100,
        },
        4,
    })

    -- test that unpack int32 for number of times specified by length modifier
    v = {}
    consumed, err, again = unpack(v, 'i3',
                                  htonl(200) .. htonl(201) .. htonl(202))
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            200,
            201,
            202,
        },
        12,
    })

    -- test that unpack int32 for number of times specified by preceding int32
    v = {}
    consumed, err, again = unpack(v, 'ii*',
                                  htonl(3) .. htonl(200) .. htonl(201) ..
                                      htonl(202))
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            3,
            200,
            201,
            202,
        },
        16,
    })

    -- test that return again=true if message length is less than 4 bytes
    v = {}
    consumed, err, again = unpack(v, 'i', '\0\0\0')
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {},
        nil,
        nil,
        true,
    })

    -- test that throw error if format with '*' length modifier without preceding number format
    err = assert.throws(unpack, v, 'si*', 'hello\0' .. htons(100))
    assert.match(err, 'must be preceded by the integer type specifier')
end

function testcase.unpack_str()
    -- test that unpack null-terminated string
    local v = {}
    local consumed, err, again = unpack(v, 's', 'abc\0')
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            'abc',
        },
        4,
    })

    -- test that unpack multiple null-terminated string
    v = {}
    consumed, err, again = unpack(v, 'sss', 'abc\0' .. 'def\0' .. 'ghi\0')
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            'abc',
            'def',
            'ghi',
        },
        12,
    })

    -- test that unpack null-terminated string for number of times specified by length modifier
    v = {}
    consumed, err, again = unpack(v, 's3', 'abc\0' .. 'def\0' .. 'ghi\0')
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            'abc',
            'def',
            'ghi',
        },
        12,
    })

    -- test that return again=true if string is not null-terminated
    v = {}
    consumed, err, again = unpack(v, 's', 'abc')
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {},
        nil,
        nil,
        true,
    })

    -- test that throw error if format with '*' length modifier without preceding number format
    err = assert.throws(unpack, v, 's*', 'hello\0' .. htons(100))
    assert.match(err, 'must be specified only for the type specifier')
end

function testcase.unpack_byte()
    -- test that unpack byte string
    local v = {}
    local consumed, err, again = unpack(v, 'b3', 'abc')
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            'abc',
        },
        3,
    })

    -- test that return again=true if remaining message length is not enough
    v = {}
    consumed, err, again = unpack(v, 'b2147483647', '')
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {},
        nil,
        nil,
        true,
    })

    -- test that unpack byte string that length is specified by int16 value
    v = {}
    consumed, err, again = unpack(v, 'hb*', htons(3) .. 'abc')
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            3,
            'abc',
        },
        5,
    })

    -- test that unpack 0 byte string
    v = {}
    consumed, err, again = unpack(v, 'hb*', htons(0) .. 'abc')
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            0,
            '',
        },
        2,
    })

    -- test that throw error if format without length modifier
    err = assert.throws(unpack, v, 'b', 'abc')
    assert.match(err, 'must be followed by length modifier')

    -- test that throw error if format with '*' length modifier without preceding number format
    err = assert.throws(unpack, v, 'b*', 'abc')
    assert.match(err, 'must be preceded by the integer type specifier')

    -- test that throw error if length modifier is zero
    err = assert.throws(unpack, v, 'b0', '')
    assert.match(err, 'length modifier must be greater than zero')

    -- test that throw error if length modifier is greater than INT32_MAX
    err = assert.throws(unpack, v, 'b2147483648', '')
    assert.match(err, 'length modifier must be less than or equal to INT32_MAX')

end

function testcase.unpack_length()
    -- test that unpack length value
    local v = {}
    local consumed, err, again = unpack(v, 'L', htonl(4))
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            4,
        },
        4,
    })

    -- test that remaing message length is specified by length value
    v = {}
    consumed, err, again = unpack(v, 'Lb*', htonl(7) .. 'abc')
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            7,
            'abc',
        },
        7,
    })

    -- test that return again=true if remaining message length is not enough
    v = {}
    consumed, err, again = unpack(v, 'L', htonl(8))
    assert.equal({
        v,
        consumed,
        err,
        again,
    }, {
        {
            8,
        },
        nil,
        nil,
        true,
    })

    -- test that return error if length value is negative
    v = {}
    consumed, err, again = unpack(v, 'L', htonl(-1))
    assert.equal(v, {
        -1,
    })
    assert.is_nil(consumed)
    assert.match(err,
                 'message length must be greater than or equal to its own length')
    assert.is_nil(again)

    -- test that return error if length is not sufficient to unpack string data
    v = {}
    consumed, err, again = unpack(v, 'Ls', htonl(4 + 3) .. 'hello\0')
    assert.equal(v, {
        7,
    })
    assert.is_nil(consumed)
    assert.match(err, 'insufficient to unpack')
    assert.is_nil(again)

    -- test that throw error if 'L' specified multiple times
    err = assert.throws(unpack, v, 'LL', htonl(4) .. htonl(4))
    assert.match(err, 'must be specified only once')

    -- test that return error if format with digit length modifier
    err = assert.throws(unpack, v, 'L1', htonl(-1))
    assert.match(err, 'digit length modifier can not be specified')

    -- test that return error if format with '*' length modifier
    err = assert.throws(unpack, v, 'L*', htonl(-1))
    assert.match(err, 'must be specified only for the type specifier')
end

function testcase.unpack_unknown_type()
    -- test that throw error if unknown type
    local v = {}
    local err = assert.throws(unpack, v, 'z', '')
    assert.match(err, "invalid format string: unknown type specifier 'z'")
end

