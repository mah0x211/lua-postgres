require('luacov')
local testcase = require('testcase')
local assert = require('assert')
local setenv = require('setenv')
local parse_conninfo = require('postgres.conninfo')

local DEFAULT_ENV = {}

function testcase.before_all()
    for k, v in pairs({
        PGSSLMODE = 'disable',
        PGTZ = 'UTC',
        PGDATABASE = 'postgres',
    }) do
        DEFAULT_ENV[k] = os.getenv(k)
        setenv(k, v)
    end
end

function testcase.after_each()
    for k, v in pairs(DEFAULT_ENV) do
        setenv(k, v)
    end
end

function testcase.parse_conninfo()
    -- test that get a conninfo object and normalized conninfo string
    setenv('PGSSLMODE', 'disable')
    setenv('PGTZ', 'UTC')
    setenv('PGDATABASE')
    local info, err, conninfo = parse_conninfo(
                                    'postgres://user:pass@host:1234/dbname?connect_timeout=1')
    assert.is_nil(err)
    assert.equal(conninfo,
                 'postgres://user:pass@host:1234/dbname?connect_timeout=1&sslmode=disable&timezone=UTC')
    assert.equal({
        conninfo = conninfo,
        info = info,
    }, {
        conninfo = 'postgres://user:pass@host:1234/dbname?connect_timeout=1&sslmode=disable&timezone=UTC',
        info = {
            user = 'user',
            password = 'pass',
            host = 'host',
            port = '1234',
            dbname = 'dbname',
            params = {
                connect_timeout = 1,
                sslmode = 'disable',
                timezone = 'UTC',
            },
        },
    })

    -- test that can be specified a 'postgresql://' scheme, and user and
    -- password specified in query. also, user name uses as dbname if dbname is
    -- not specified.
    info, err, conninfo = parse_conninfo(
                              'postgresql://host:1234?connect_timeout=1&user=user&password=pass')
    assert.is_nil(err)
    assert.equal({
        conninfo = conninfo,
        info = info,
    }, {
        conninfo = 'postgres://user:pass@host:1234/user?connect_timeout=1&sslmode=disable&timezone=UTC',
        info = {
            user = 'user',
            password = 'pass',
            host = 'host',
            port = '1234',
            dbname = 'user',
            params = {
                connect_timeout = 1,
                sslmode = 'disable',
                timezone = 'UTC',
            },
        },
    })

    -- test that return error if scheme is not 'postgres://' or 'postgresql://'
    info, err, conninfo = parse_conninfo('http://host')
    assert.is_nil(info)
    assert.match(err,
                 'scheme must be start with "postgres://" or "postgresql://"')
    assert.is_nil(conninfo)

    -- test that return error if invalid URI character found
    info, err, conninfo =
        parse_conninfo('postgres://user:pass@example.com@user')
    assert.is_nil(info)
    assert.match(err, 'illegal character "@" found')
    assert.is_nil(conninfo)
end
