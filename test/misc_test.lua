local testcase = require('testcase')
local setenv = require('setenv')
local misc = require('postgres.misc')
local pgconn = require('postgres.pgconn')

local CONNINFO_KEYWORDS = {
    'application_name',
    'channel_binding',
    'client_encoding',
    'connect_timeout',
    'dbname',
    'fallback_application_name',
    'gssencmode',
    'gsslib',
    'host',
    'hostaddr',
    'keepalives',
    'keepalives_count',
    'keepalives_idle',
    'keepalives_interval',
    'krbsrvname',
    'options',
    'passfile',
    'password',
    'port',
    'replication',
    'requirepeer',
    'service',
    'ssl_max_protocol_version',
    'ssl_min_protocol_version',
    'sslcert',
    'sslcompression',
    'sslcrl',
    'sslcrldir',
    'sslkey',
    'sslmode',
    'sslpassword',
    'sslrootcert',
    'sslsni',
    'target_session_attrs',
    'tcp_user_timeout',
    'user',
}

function testcase.conninfo_defaults()
    -- test that get list of default conninfo
    local conninfo = assert(misc.conninfo_defaults())
    assert.is_table(conninfo)
    for _, keyword in ipairs(CONNINFO_KEYWORDS) do
        assert.is_table(conninfo[keyword])
    end
end

function testcase.conninfo_parse()
    -- test that parse conninfo string
    local conninfo = assert(misc.conninfo_parse(
                                'host=localhost port=5432 dbname=mydb connect_timeout=10'))
    assert.is_table(conninfo)
    for _, keyword in ipairs(CONNINFO_KEYWORDS) do
        assert.is_table(conninfo[keyword])
    end

    -- test that can parse empty-string
    conninfo = assert(misc.conninfo_parse(''))
    assert.is_table(conninfo)
    for _, keyword in ipairs(CONNINFO_KEYWORDS) do
        assert.is_table(conninfo[keyword])
    end

    -- test that throws an error if conninfo argument is not string
    local err = assert.throws(misc.conninfo_parse)
    assert.match(err, 'string expected,')
end

function testcase.ping()
    -- test that ping to server
    assert.equal(misc.ping(), "ok")
end

function testcase.is_threadsafe()
    -- test that return true if libpq is thread-safe
    assert.is_boolean(misc.is_threadsafe())
end

function testcase.escape_bytea_conn()
    local c = assert(pgconn())

    -- test that escape bytea string
    local esc = assert(c:escape_bytea_conn('hello ワールド'))
    local str = assert(misc.unescape_bytea(esc))
    assert.equal(str, 'hello ワールド')
end

function testcase.mblen()
    -- test that determine the length of a multibyte character
    assert.equal(misc.mblen('abc', 'utf8'), 1)
    assert.equal(misc.mblen('あabc', 'utf8'), 3)

    -- test that return error if encoding name is invalid
    local len, err = misc.mblen('abc', 'utf9')
    assert.is_nil(len)
    assert.match(err, 'invalid encoding name')
end

function testcase.mblen_bounded()
    -- test that same of mblen, but not more than the distance to the end of string s
    assert.equal(misc.mblen_bounded('abc', 'UTF-8'), 1)
    assert.equal(misc.mblen_bounded('あabc', 'UTF-8'), 3)

    -- test that return error if encoding name is invalid
    local len, err = misc.mblen_bounded('abc', 'utf9')
    assert.is_nil(len)
    assert.match(err, 'invalid encoding name')
end

function testcase.dsplen()
    -- test that determine display length of multibyte encoded char at *s
    assert.is_int(misc.dsplen('abc', 'UTF-8'))
    assert.is_int(misc.dsplen('あabc', 'UTF-8'))

    -- test that return error if encoding name is invalid
    local len, err = misc.dsplen('abc', 'utf9')
    assert.is_nil(len)
    assert.match(err, 'invalid encoding name')
end

function testcase.env2encoding()
    -- test that get an encoding id from PGCLIENTENCODING envvar
    assert.equal(misc.env2encoding(), 0)
    setenv('PGCLIENTENCODING', 'UTF-8')
    assert.not_equal(misc.char_to_encoding('UTF-8'), 0)
    assert.equal(misc.env2encoding(), misc.char_to_encoding('UTF-8'))
end

function testcase.encrypt_password()
    -- test that encrypt a string of password/user combinations
    assert.match(misc.encrypt_password('foo', 'bar'), '^md5[a-f0-9]+', false)
end

function testcase.char_to_encoding()
    -- test that get a encoding type as string from
    assert.equal(misc.char_to_encoding('SQL_ASCII'), 0)
end

function testcase.encoding_to_char()
    -- test that get a encoding type as string from
    assert.equal(misc.encoding_to_char(0), 'SQL_ASCII')
end

function testcase.valid_server_encoding_id()
    -- test that get a encoding type as string from
    assert.is_true(misc.valid_server_encoding_id(0))
    assert.is_false(misc.valid_server_encoding_id(-123))
end

