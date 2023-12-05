require('luacov')
local testcase = require('testcase')
local assert = require('assert')
local htonl = require('postgres.htonl')
local encode = require('postgres.message').encode.authentication
local decode = require('postgres.message').decode.authentication

function testcase.encode()
    -- test that throw error when unknown type
    local err = assert.throws(encode, 'unknown')
    assert.match(err, 'unsupported Authentication message')
end

function testcase.decode()
    -- test that return again=true if message length is less than 1
    local msg, err, again = decode('')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message is not Authentication message
    msg, err, again = decode('hello')
    assert.is_nil(msg)
    assert.match(err, 'invalid Authentication message')
    assert.is_nil(again)

    -- test that return again=true if message length is less than 9
    msg, err, again = decode('R')
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return again=true if message length is less than specified length
    msg, err, again = decode('R' .. htonl(100) .. htonl(0))
    assert.is_nil(msg)
    assert.is_nil(err)
    assert.is_true(again)

    -- test that return error if message is unsupported Authentication message
    msg, err, again = decode('R' .. htonl(8) .. htonl(100))
    assert.is_nil(msg)
    assert.match(err, 'unsupported Authentication message')
    assert.is_nil(again)
end

function testcase.encode_decode_AuthenticationOK()
    -- test that decode AuthenticationOk message
    local s = encode('AuthenticationOk')
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationOk',
    })
end

function testcase.encode_decode_AuthenticationKerberosV5()
    -- test that decode AuthenticationKerberosV5 message
    local s = encode('AuthenticationKerberosV5')
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationKerberosV5',
    })
end

function testcase.encode_decode_AuthenticationCleartextPassword()
    -- test that decode AuthenticationClearTextPassword message
    local s = encode('AuthenticationCleartextPassword')
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationCleartextPassword',
    })
end

function testcase.encode_decode_AuthenticationMD5Password()
    -- test that decode AuthenticationMD5Password message
    local salt = 'salt'
    local s = encode('AuthenticationMD5Password', salt)
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationMD5Password',
        salt = salt,
    })

    -- test that throw error when salt is not 4 bytes length string
    local err = assert.throws(encode, 'AuthenticationMD5Password', {})
    assert.match(err, 'salt must be 4 bytes string')
end

function testcase.encode_decode_AuthenticationSCMCredential()
    -- test that decode AuthenticationSCMCredential message
    local s = encode('AuthenticationSCMCredential')
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationSCMCredential',
    })
end

function testcase.encode_decode_AuthenticationGSS()
    -- test that decode AuthenticationGSS message
    local s = encode('AuthenticationGSS')
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationGSS',
    })
end

function testcase.encode_decode_AuthenticationGSSContinue()
    -- test that decode AuthenticationGSSContinue message
    local data = 'data'
    local s = encode('AuthenticationGSSContinue', data)
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationGSSContinue',
        data = data,
    })

    -- test that throw error when data is not string
    local err = assert.throws(encode, 'AuthenticationGSSContinue', {})
    assert.match(err, 'data must be string')
end

function testcase.encode_decode_AuthenticationSSPI()
    -- test that decode AuthenticationSSPI message
    local s = encode('AuthenticationSSPI')
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationSSPI',
    })
end

function testcase.encode_decode_AuthenticationSASL()
    -- test that decode AuthenticationSASL message
    local s = encode('AuthenticationSASL', 'foo', 'bar')
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationSASL',
        names = {
            'foo',
            'bar',
        },
    })

    -- test that throw error when mechanisms is not string
    local err = assert.throws(encode, 'AuthenticationSASL', {})
    assert.match(err, 'name must be string')
end

function testcase.encode_decode_AuthenticationSASLContinue()
    -- test that decode AuthenticationSASLContinue message
    local data = 'data'
    local s = encode('AuthenticationSASLContinue', data)
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationSASLContinue',
        data = data,
    })

    -- test that throw error when data is not string
    local err = assert.throws(encode, 'AuthenticationSASLContinue', {})
    assert.match(err, 'data must be string')
end

function testcase.encode_decode_AuthenticationSASLFinal()
    -- test that decode AuthenticationSASLFinal message
    local data = 'data'
    local s = encode('AuthenticationSASLFinal', data)
    local msg = assert(decode(s))
    assert.match(msg, 'postgres.message.authentication')
    assert.contains(msg, {
        consumed = #s,
        type = 'AuthenticationSASLFinal',
        data = data,
    })

    -- test that throw error when data is not string
    local err = assert.throws(encode, 'AuthenticationSASLFinal', {})
    assert.match(err, 'outcome "additional data" must be string')
end

