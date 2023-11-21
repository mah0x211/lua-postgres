--
-- Copyright (C) 2023 Masatoshi Fukunaga
--
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
--
-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.
--
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
-- THE SOFTWARE.
--
--- assign to local
local concat = table.concat
local sort = table.sort
local sub = string.sub
local find = string.find
local getenv = os.getenv
local pairs = pairs
local type = type
local errorf = require('error').format
local parse_url = require('url').parse

local PQconninfoOptions = {
    service = 'PGSERVICE', -- Database-Service
    user = 'PGUSER', -- Database-User
    password = 'PGPASSWORD', -- Database-Password
    passfile = 'PGPASSFILE', -- Database-Password-File
    channel_binding = 'PGCHANNELBINDING', -- Channel-Binding (default: 'disable')
    connect_timeout = 'PGCONNECT_TIMEOUT', -- Connect-timeout
    dbname = 'PGDATABASE', -- Database-Name
    host = 'PGHOST', -- Database-Host
    hostaddr = 'PGHOSTADDR', -- Database-Host-IP-Address
    port = 'PGPORT', -- Database-Port
    client_encoding = 'PGCLIENTENCODING', -- Client-Encoding
    options = 'PGOPTIONS', -- Backend-Options (default: '')
    application_name = 'PGAPPNAME', -- Application-Name

    --
    -- ssl options are allowed even without client SSL support because the
    -- client can still handle SSL modes "disable" and "allow". Other
    -- parameters have no effect on non-SSL connections, so there is no reason
    -- to exclude them since none of them are mandatory.
    --
    sslmode = 'PGSSLMODE', -- SSL-Mode (default: 'disable')
    sslcompression = 'PGSSLCOMPRESSION', -- SSL-Compression (default: '0')
    sslcert = 'PGSSLCERT', -- SSL-Client-Cert
    sslkey = 'PGSSLKEY', -- SSL-Client-Key
    sslcertmode = 'PGSSLCERTMODE', -- SSL-Client-Cert-Mode
    sslrootcert = 'PGSSLROOTCERT', -- SSL-Root-Certificate
    sslcrl = 'PGSSLCRL', -- SSL-Revocation-List
    sslcrldir = 'PGSSLCRLDIR', -- SSL-Revocation-List-Dir
    sslsni = 'PGSSLSNI', -- SSL-SNI (default: '1')
    requirepeer = 'PGREQUIREPEER', -- Require-Peer
    require_auth = 'PGREQUIREAUTH', -- Require-Auth
    ssl_min_protocol_version = 'PGSSLMINPROTOCOLVERSION', -- SSL-Minimum-Protocol-Version (default: 'TLSv1.2')
    ssl_max_protocol_version = 'PGSSLMAXPROTOCOLVERSION', -- SSL-Maximum-Protocol-Version

    --
    -- As with SSL, all GSS options are exposed even in builds that don't have
    -- support.
    --
    gssencmode = 'PGGSSENCMODE', -- GSSENC-Mode (default: 'disable')
    -- Kerberos and GSSAPI authentication support specifying the service name
    krbsrvname = 'PGKRBSRVNAME', -- Kerberos-service-name (default: 'postgres')
    gsslib = 'PGGSSLIB', -- GSS-library,
    gssdelegation = 'PGGSSDELEGATION', -- GSS-delegation (default: '0')
    target_session_attrs = 'PGTARGETSESSIONATTRS', -- Target-Session-Attrs, (default: 'any')
    load_balance_hosts = 'PGLOADBALANCEHOSTS', -- Load-Balance-Hosts (default: 'disable')

    --
    -- common user-interface settings
    --
    datestyle = 'PGDATESTYLE',
    timezone = 'PGTZ',

    --
    -- internal performance-related settings
    --
    geqo = 'PGGEQO',
}

local HOSTPATHSPEC = {
    -- userspec
    user = true,
    password = true,
    -- hostspec
    host = true,
    port = true,
    -- pathspec
    dbname = true,
}
--- parse_conninfo
--- @param conninfo string
--- @return table? info
--- @return any err
--- @return string conninfo normalized connection string
local function parse_conninfo(conninfo)
    assert(type(conninfo) == 'string', 'conninfo must be string')

    local info = {}
    local params = {}
    if conninfo ~= '' then
        if not find(conninfo, '^postgres://') and
            not find(conninfo, '^postgresql://') then
            error('invalid connection string')
        end

        -- parse URI
        local uri, pos, err = parse_url(conninfo, true)
        if err then
            return nil, errorf(
                       'invalid connection-uri character %q found at %d', err,
                       pos + 1)
        end
        -- userspec
        info.user = uri.user
        info.password = uri.password
        -- hostspec
        info.host = uri.hostname
        info.port = uri.port
        -- pathspec
        if find(uri.path or '', '^/.+') then
            info.dbname = sub(uri.path, 2)
        end
        -- paramspec
        if uri.query_params then
            params = uri.query_params
        end

        -- overwrite with query parameters
        for k in pairs(HOSTPATHSPEC) do
            local v = params[k]
            if v then
                info[k] = v[#v]
                -- remove from params
                params[k] = nil
            end
        end
    end

    -- fill default values with environment variables
    for k, v in pairs(PQconninfoOptions) do
        local target = HOSTPATHSPEC[k] and info or params
        if target[k] == nil then
            target[k] = getenv(v)
        end
    end

    -- fill default values
    for k, v in pairs({
        host = '127.0.0.1',
        port = '5432',
        dbname = info.user,
    }) do
        if info[k] == nil then
            info[k] = v
        end
    end

    -- TODO: check the following fields;
    -- * host (comma-separated list of host names)
    -- * hostaddr (comma-separated list of IP addresses)
    -- * port (comma-separated list of ports)
    -- * user (default: username of effective user ID)
    -- * dbname (default: user)
    -- * password (PGPASSFILE or ~/.pgpass)
    -- * require_auth (comma-separated list of authentication methods)
    -- * channel_binding (validate)
    -- * sslrootcert (file path)
    -- * ssmode (validate)
    -- * ssl_min_protocol_version (validate)
    -- * ssl_max_protocol_version (validate)
    -- * ssl_cert_mode (validate)
    -- * gssencmode (validate)
    -- * target_session_attrs (validate)
    -- * load_balance_hosts (validate)
    -- * client_encoding (validate)

    if params.connect_timeout then
        params.connect_timeout = tonumber(params.connect_timeout)
        if not params.connect_timeout then
            return nil, errorf('invalid connect_timeout parameter')
        end
    end

    -- build connection string
    local arr = {
        'postgres://',
    }
    -- userspec
    if info.user then
        arr[#arr + 1] = info.user
        if info.password then
            arr[#arr + 1] = ':' .. info.password
        end
        arr[#arr + 1] = '@'
    end

    -- hostspec
    arr[#arr + 1] = info.host .. ':' .. info.port
    -- dbname
    if info.dbname then
        arr[#arr + 1] = '/' .. info.dbname
    end
    conninfo = concat(arr)

    -- paramspec
    arr = {}
    for k, v in pairs(params) do
        arr[#arr + 1] = k .. '=' .. v
    end

    if #arr > 0 then
        sort(arr)
        conninfo = conninfo .. '?' .. concat(arr, '&')
    end

    info.params = params
    return info, nil, conninfo
end

return parse_conninfo
