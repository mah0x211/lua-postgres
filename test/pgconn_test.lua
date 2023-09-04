local testcase = require('testcase')
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

function testcase.connect()
    -- test that connect to server
    local c = assert(pgconn())
    assert.match(c, '^postgres.pgconn: ', false)

    -- test that connect to server in non-blocking mode
    c = assert(pgconn(nil, true))
    assert.match(c, '^postgres.pgconn: ', false)

    -- test that throws an error if conninfo argument is not string
    local err = assert.throws(pgconn, true)
    assert.match(err, 'string expected,')

    -- test that throws an error if nonblock argument is not boolean
    err = assert.throws(pgconn, nil, 'true')
    assert.match(err, 'boolean expected,')
end

function testcase.conninfo()
    local c = assert(pgconn())

    -- test that get conninfo
    local conninfo = assert(c:conninfo())
    assert.is_table(conninfo)
    for _, keyword in ipairs(CONNINFO_KEYWORDS) do
        assert.is_table(conninfo[keyword])
    end

    -- test that throws an error after connection is finished
    c:finish()
    local err = assert.throws(c.conninfo, c)
    assert.match(err, 'attempt to use a freed object')
end

function testcase.connect_poll()
    local c = assert(pgconn())

    -- test that return a PGRES_POLLING_OK status
    assert.equal(c:connect_poll(), 'ok')
end

function testcase.db()
    local conninfo = assert(misc.conninfo_defaults())
    local c = assert(pgconn())

    -- test that return db name
    assert.equal(c:db(), conninfo.dbname.val)
end

function testcase.user()
    local conninfo = assert(misc.conninfo_defaults())
    local c = assert(pgconn())

    -- test that return user name
    assert.equal(c:user(), conninfo.user.val)
end

function testcase.pass()
    local conninfo = assert(misc.conninfo_defaults())
    local c = assert(pgconn())

    -- test that return password
    assert.equal(c:pass(), conninfo.password.val or '')
end

function testcase.host()
    local conninfo = assert(misc.conninfo_defaults())
    local c = assert(pgconn())

    -- test that return host
    assert.equal(c:host(), conninfo.host.val)
end

function testcase.hostaddr()
    local c = assert(pgconn())

    -- test that return hostaddr
    assert.is_string(c:hostaddr())
end

function testcase.port()
    local conninfo = assert(misc.conninfo_defaults())
    local c = assert(pgconn())

    -- test that return port number
    assert.equal(c:port(), conninfo.port.val)
end

function testcase.options()
    local c = assert(pgconn())

    -- test that return options
    assert.equal(c:options(), '')
end

function testcase.status()
    local c = assert(pgconn())

    -- test that return status
    assert.equal(c:status(), 'ok')
end

function testcase.transaction_status()
    local c = assert(pgconn())

    -- test that return transaction status
    assert.equal(c:transaction_status(), 'idle')
end

function testcase.parameter_status()
    local c = assert(pgconn())

    -- test that return parameter status
    assert.equal(c:parameter_status('server_encoding'), 'UTF8')
end

function testcase.protocol_version()
    local c = assert(pgconn())

    -- test that return protocol version
    assert.equal(c:protocol_version(), 3)
end

function testcase.server_version()
    local c = assert(pgconn())

    -- test that return server version
    assert.is_uint(c:server_version())
end

function testcase.error_message()
    local c = assert(pgconn())

    -- test that return error message or nil
    assert.is_nil(c:error_message())
end

function testcase.socket()
    local c = assert(pgconn())

    -- test that return socket descriptor
    assert.greater(c:socket(), 2)
end

function testcase.backend_pid()
    local c = assert(pgconn())

    -- test that return backend process-id
    assert.greater(c:backend_pid(), 1)
end

function testcase.pipeline_status()
    local c = assert(pgconn())

    -- test that return pipeline status
    assert.equal(c:pipeline_status(), 'off')
end

function testcase.connection_needs_password()
    local c = assert(pgconn())

    -- test that return false
    assert.is_false(c:connection_needs_password())
end

function testcase.connection_used_password()
    local c = assert(pgconn())

    -- test that is connection used password
    assert.is_true(c:connection_used_password())
end

function testcase.client_encoding()
    local c = assert(pgconn())

    -- test that get client encoding
    assert.equal(c:client_encoding(), 'UTF8')
end

function testcase.set_client_encoding()
    local c = assert(pgconn())

    -- test that return true if argument is valid encoding string
    local ok, err = assert(c:set_client_encoding('SJIS'))
    assert.is_true(ok)
    assert.is_nil(err)

    -- test that return false if argument is invalid encoding string
    ok, err = c:set_client_encoding('invalid')
    assert.is_false(ok)
    assert.match(err, 'invalid value for parameter')
    assert.equal(c:error_message(), err.message.message)

    -- test that throws an error if argument is not string
    err = assert.throws(c.set_client_encoding, c, {})
    assert.match(err, 'string expected,')
end

function testcase.ssl_in_use()
    local c = assert(pgconn())

    -- test that return false if connect without ssl
    assert.is_false(c:ssl_in_use())
end

function testcase.ssl_attribute()
    local c = assert(pgconn())

    -- test that get ssl attribute
    assert.is_nil(c:ssl_attribute('library'))
end

function testcase.ssl_attribute_names()
    local c = assert(pgconn())

    -- test that return attribute names
    assert.equal(c:ssl_attribute_names(), {
        'library',
        'key_bits',
        'cipher',
        'compression',
        'protocol',
    })
end

function testcase.set_error_verbosity()
    local c = assert(pgconn())

    -- test that set error verbosity and return previous verbosity settings
    assert.is_string(c:set_error_verbosity('terse'))
    assert.equal(c:set_error_verbosity('default'), 'terse')
end

function testcase.set_error_context_visibility()
    local c = assert(pgconn())

    -- test that set error verbosity and return previous verbosity settings
    assert.is_string(c:set_error_context_visibility('never'))
    assert.equal(c:set_error_context_visibility('errors'), 'never')
end

function testcase.set_notice_processor()
    local c = assert(pgconn())
    local ncall = 0

    -- test that set notice processor function
    c:set_notice_processor(function(...)
        ncall = ncall + 1
        assert.equal({
            ...,
        }, {
            'foo',
            'bar',
            'baz',
            'hello',
        })
    end, 'foo', 'bar', 'baz')
    assert.is_true(c:call_notice_processor('hello'))
    assert.equal(ncall, 1)

    -- test that set default processor function
    c:set_notice_processor()
    assert.is_false(c:call_notice_processor('hello'))
    assert.equal(ncall, 1)

    -- test that throws an error if argument is not string
    local err = assert.throws(c.call_notice_processor, c)
    assert.match(err, 'string expected,')
end

function testcase.set_notice_receiver()
    local c = assert(pgconn())
    local res = assert(c:make_empty_result())
    local ncall = 0

    -- TODO: test with postgres.pgresult object
    -- test that set notice receiver function
    c:set_notice_receiver(function(...)
        ncall = ncall + 1
        assert.equal({
            ...,
        }, {
            'foo',
            'bar',
            'baz',
            res,
        })
    end, 'foo', 'bar', 'baz')
    assert.is_true(c:call_notice_receiver(res))
    assert.equal(ncall, 1)

    -- test that set default processor function
    c:set_notice_receiver()
    assert.is_false(c:call_notice_receiver(res))
    assert.equal(ncall, 1)

    -- test that throws an error if argument is not string
    local err = assert.throws(c.call_notice_receiver, c)
    assert.match(err, 'postgres.pgresult expected,')
end

function testcase.trace()
    local c = assert(pgconn())
    local f = assert(io.tmpfile())

    -- test that set trace file and return associated old file
    assert.is_nil(c:trace(f))
    assert.equal(c:trace(f), f)

    -- test that throws an error if argument is not file
    local err = assert.throws(c.trace, c)
    assert.match(err, 'FILE%* expected,', false)
end

function testcase.untrace()
    local c = assert(pgconn())
    local f = assert(io.tmpfile())
    c:trace(f)

    -- test that untrace and return associated file
    assert.equal(c:untrace(), f)
    assert.is_nil(c:untrace())
end

function testcase.set_trace_flags()
    local c = assert(pgconn())
    local f = assert(io.tmpfile())
    c:trace(f)

    -- test that set flags
    c:set_trace_flags('suppress_timestamps', 'regress_mode')

    -- test that throws an error if argument is not integer
    local err = assert.throws(c.set_trace_flags, c, {})
    assert.match(err, 'string expected,')
end

function testcase.exec()
    local c = assert(pgconn())

    -- test that exec query
    local res = assert(c:exec('SELECT 1 + 2'))
    assert.match(res, '^postgres.pgresult: ', false)

    -- test that throws an error if query is not string
    local err = assert.throws(c.exec, c)
    assert.match(err, 'string expected,')
end

function testcase.exec_params()
    local c = assert(pgconn())

    -- test that exec command with params
    local res = assert(c:exec_params('SELECT $1 + $2', 5, 10))
    assert.match(res, '^postgres.pgresult: ', false)

    -- test that exec command without params
    res = assert(c:exec_params('SELECT 1 + 2'))
    assert.match(res, '^postgres.pgresult: ', false)

    -- test that throws an error if command is not string
    local err = assert.throws(c.exec_params, c)
    assert.match(err, 'string expected,')

    -- test that throws an error if param is not string|boolean|number
    err = assert.throws(c.exec_params, c, 'SELECT 1 + 2', 'hello', true, false,
                        1, 1.1, -1, {})
    assert.match(err, '<table> param is not supported')
end

function testcase.send_query()
    local c = assert(pgconn())

    -- test that send query
    assert(c:send_query('SELECT 1 + 2'))

    -- test that throws an error if query is not string
    local err = assert.throws(c.send_query, c)
    assert.match(err, 'string expected,')
end

function testcase.prepare_and_exec_prepare()
    local c = assert(pgconn())

    -- test that register prepared statement
    local res = assert(c:prepare('example', "SELECT $1|| ' ' || $2 AS result"))
    assert.equal(res:status(), 'command_ok')

    -- test that cannot register prepared statement with the same name
    res = assert(c:prepare('example', 'SELECT $1 || $2 || $3'))
    assert.equal(res:status(), 'fatal_error')
    assert.match(res:error_message(), 'already exists')

    -- test that execute prepared statement
    res = assert(c:exec_prepare('example', 'hello', 'world!'))
    assert.equal(res:status(), 'tuples_ok')
    assert.equal(res:get_value(1, 1), 'hello world!')
end

function testcase.send_query_params()

    -- test that send command with params
    local c = assert(pgconn())
    assert(c:send_query_params('SELECT $1 + $2', 5, 10))

    -- test that send command without params
    c = assert(pgconn())
    assert(c:send_query_params('SELECT 1 + 2'))

    -- test that throws an error if command is not string
    local err = assert.throws(c.send_query_params, c)
    assert.match(err, 'string expected,')

    -- test that throws an error if param is not string|boolean|number
    err = assert.throws(c.send_query_params, c, 'SELECT 1 + 2', 'hello', true,
                        false, 1, 1.1, -1, {})
    assert.match(err, '<table> param is not supported')
end

function testcase.set_single_row_mode()
    local c = assert(pgconn())

    -- test that return false if no query in processes
    assert.is_false(c:set_single_row_mode())

    -- test that return false if query in processes
    assert(c:send_query('SELECT 1 + 2'))
    assert.is_true(c:set_single_row_mode())
end

function testcase.get_result()
    local c = assert(pgconn())
    assert(c:send_query('SELECT 1 + 2'))

    -- test that get query result
    local res, err = assert(c:get_result())
    assert.is_nil(err)
    assert.match(res, '^postgres%.pgresult: ', false)
    res:clear()

    -- test that get nil if no query in processes
    res, err = c:get_result()
    assert.is_nil(res)
    assert.is_nil(err)
end

function testcase.is_busy()
    local c = assert(pgconn())

    -- test that false if no query in process
    assert.is_false(c:is_busy())

    -- test that true if query in process
    assert(c:send_query('SELECT pg_sleep(1)'))
    assert.is_true(c:is_busy())
end

function testcase.consume_input()
    local c = assert(pgconn())

    -- test that true
    assert(c:consume_input())
end

function testcase.enter_pipeline_mode_and_exit_pipeline_mode()
    local c = assert(pgconn())

    -- test that enter the pipeline mode
    assert.equal(c:pipeline_status(), 'off')
    assert(c:enter_pipeline_mode())
    assert.equal(c:pipeline_status(), 'on')

    -- test that method can be called twice
    assert(c:enter_pipeline_mode())

    -- test that exit the pipeline mode
    assert(c:exit_pipeline_mode())
    assert.equal(c:pipeline_status(), 'off')

    -- test that method can be called twice
    assert(c:exit_pipeline_mode())

    -- test that cannot use send_query method in pipeline mode
    assert(c:enter_pipeline_mode())
    local ok, err = c:send_query('SELECT 1 + 2')
    assert.is_false(ok)
    assert.match(err, 'not allowed in pipeline mode')

    -- test that cannot exit pipeline mode if the query in processing
    assert(c:send_query_params('SELECT $1 + $2', 1, 2))
    ok, err = c:exit_pipeline_mode()
    assert.is_false(ok)
    assert.match(err, 'cannot exit pipeline mode')
end

function testcase.pipeline_sync()
    local c = assert(pgconn())
    assert(c:enter_pipeline_mode())

    -- test that true if currently in pipeline mode
    assert(c:pipeline_sync())

    -- test that false if the query in processing
    c = assert(pgconn())
    local ok, err = c:pipeline_sync()
    assert.is_false(ok)
    assert.match(err, 'not in pipeline mode')
end

function testcase.send_flush_request()
    local c = assert(pgconn())

    -- test that true
    assert(c:send_flush_request())
end

function testcase.notifies()
    local c = assert(pgconn())
    local listener = assert(pgconn())
    local res = assert(listener:exec('LISTEN hello'))
    assert.equal(res:status(), 'command_ok')

    -- test that return nil if no notification received
    assert.is_nil(listener:notifies())

    -- test that return notification
    assert(c:exec('NOTIFY hello'))
    assert.equal(res:status(), 'command_ok')
    assert(c:exec("NOTIFY hello, 'world'"))
    assert.equal(res:status(), 'command_ok')
    local i = 0
    while i < 2 do
        local notify, err = listener:notifies()
        assert.is_nil(err)
        if notify then
            i = i + 1
            if i == 1 then
                assert.contains(notify, {
                    extra = '',
                    relname = 'hello',
                })
            else
                assert.contains(notify, {
                    extra = 'world',
                    relname = 'hello',
                })
            end
        end
    end
end

function testcase.put_copy_data()
    local c = assert(pgconn())
    local res = assert(c:exec([[
        CREATE TEMP TABLE copy_test (
            id serial,
            str varchar
        )
    ]]))
    assert.equal(res:status(), 'command_ok')

    -- test that copy data from stdin
    res = assert(c:exec([[
        COPY copy_test (str) FROM STDIN
        ]]))
    assert.equal(res:status(), 'copy_in')
    assert(c:put_copy_data(table.concat({
        'foo',
        'bar',
        'baz',
    }, '\n')))
    assert(c:put_copy_end())
    res = assert(c:get_result())
    assert.equal(res:status(), 'command_ok')

    -- test that copy data from stdout
    res = assert(c:exec([[
        COPY copy_test TO STDOUT
    ]]))
    assert.equal(res:status(), 'copy_out')
    local rows = {}
    repeat
        local line, err, again = c:get_copy_data()
        assert(not err, err)
        if line then
            rows[#rows + 1] = line
            again = true
        end
    until again == nil
    assert.equal(rows, {
        '1\tfoo\n',
        '2\tbar\n',
        '3\tbaz\n',
    })
end

function testcase.set_nonblocking()
    local c = assert(pgconn())

    -- test that return ture
    assert(c:set_nonblocking(true))

    -- test that can be called more than once
    assert(c:set_nonblocking(true))
    assert(c:set_nonblocking(false))
    assert(c:set_nonblocking(false))

    -- test that throws an error if argument is not boolean
    local err = assert.throws(c.set_nonblocking, c, {})
    assert.match(err, 'boolean expected,')
end

function testcase.is_nonblocking()
    local c = assert(pgconn())

    -- test that return false if it is not non-blocking mode
    assert.is_false(c:is_nonblocking())

    -- test that return true if it is non-blocking mode
    assert(c:set_nonblocking(true))
    assert.is_true(c:is_nonblocking())
end

function testcase.flush()
    local c = assert(pgconn())

    -- test that return true
    assert.is_true(c:flush())
end

function testcase.make_empty_result()
    local c = assert(pgconn())

    -- test that creat empty result
    local res = c:make_empty_result('copy_out')
    assert.match(res, '^postgres%.pgresult: ', false)
    assert.match(res:status(), 'copy_out')

    -- test that throws an error if argument is invalid
    local err = assert.throws(c.make_empty_result, c, 'unknown status')
    assert.match(err, 'invalid option')
end

function testcase.escape_string_conn()
    local c = assert(pgconn())

    -- test that escape string
    local str = assert(c:escape_string_conn("t' OR 't' = 't"))
    assert.equal(str, "t'' OR ''t'' = ''t")
end

function testcase.escape_literal()
    local c = assert(pgconn())

    -- test that escape literal string
    local str = assert(c:escape_literal("t' OR 't' = 't"))
    assert.equal(str, "'t'' OR ''t'' = ''t'")
end

function testcase.escape_identifier()
    local c = assert(pgconn())

    -- test that escape identifer string
    local str = assert(c:escape_identifier('hello_world'))
    assert.equal(str, '"hello_world"')
end

function testcase.encrypt_password_conn()
    local c = assert(pgconn())

    -- test that encrypt with md5 algorithm
    local str, err = assert(c:encrypt_password_conn('foo', 'bar', 'md5'))
    assert.match(str, '^md5', false)
    assert.is_nil(err)

    -- test that encrypt with scram-sha-256 algorithm
    str, err = assert(c:encrypt_password_conn('foo', 'bar', 'scram-sha-256'))
    assert.match(str, '^SCRAM%-SHA%-256', false)
    assert.is_nil(err)

    -- test that encrypt without algorithm
    str, err = assert(c:encrypt_password_conn('foo', 'bar'))
    assert.is_string(str)
    assert.is_nil(err)

    -- test that return error if algorithm is invalid
    str, err = c:encrypt_password_conn('foo', 'bar', 'hello')
    assert.is_nil(str)
    assert.match(err, 'unrecognized password encryption algorithm')
end

