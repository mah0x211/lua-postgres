/**
 *  Copyright (C) 2022 Masatoshi Fukunaga
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"),
 *  to deal in the Software without restriction, including without limitation
 *  the rights to use, copy, modify, merge, publish, distribute, sublicense,
 *  and/or sell copies of the Software, and to permit persons to whom the
 *  Software is furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 *  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 *  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 *  DEALINGS IN THE SOFTWARE.
 */

// lua
#include "lua_postgres.h"

// /* Support for overriding sslpassword handling with a callback */
// typedef int (*PQsslKeyPassHook_OpenSSL_type) (char *buf, int size, PGconn
// *conn); extern PQsslKeyPassHook_OpenSSL_type
// PQgetSSLKeyPassHook_OpenSSL(void); extern void
// PQsetSSLKeyPassHook_OpenSSL(PQsslKeyPassHook_OpenSSL_type hook); extern int
// PQdefaultSSLKeyPassHook_OpenSSL(char *buf, int size, PGconn *conn);

static int valid_server_encoding_id_lua(lua_State *L)
{
    int encoding = lauxh_checkinteger(L, 1);
    lua_pushboolean(L, pg_valid_server_encoding_id(encoding));
    return 1;
}

static int encoding_to_char_lua(lua_State *L)
{
    int encoding = lauxh_checkinteger(L, 1);
    lua_pushstring(L, pg_encoding_to_char(encoding));
    return 1;
}

static int char_to_encoding_lua(lua_State *L)
{
    const char *name = lauxh_checkstring(L, 1);
    lua_pushinteger(L, pg_char_to_encoding(name));
    return 1;
}

static int encrypt_password_lua(lua_State *L)
{
    const char *passwd = lauxh_checkstring(L, 1);
    const char *user   = lauxh_checkstring(L, 2);
    char *res          = NULL;

    errno = 0;
    res   = PQencryptPassword(passwd, user);
    lua_settop(L, 0);

    if (res) {
        lua_pushstring(L, res);
        PQfreemem(res);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "PQencryptPassword");
    return 2;
}

static int env2encoding_lua(lua_State *L)
{
    // Get encoding id from environment variable PGCLIENTENCODING
    lua_pushinteger(L, PQenv2encoding());
    return 1;
}

static int dsplen_lua(lua_State *L)
{
    const char *s = lauxh_checkstring(L, 1);
    int encoding  = pg_char_to_encoding(lauxh_checkstring(L, 2));

    if (encoding == -1) {
        lua_pushnil(L);
        lua_errno_new_with_message(L, EINVAL, "PQdsplen",
                                   "invalid encoding name");
        return 2;
    }

    // Determine display length of multibyte encoded char at *s
    lua_pushinteger(L, PQdsplen(s, encoding));
    return 1;
}

static int mblen_bounded_lua(lua_State *L)
{
    const char *s = lauxh_checkstring(L, 1);
    int encoding  = pg_char_to_encoding(lauxh_checkstring(L, 2));

    if (encoding == -1) {
        lua_pushnil(L);
        lua_errno_new_with_message(L, EINVAL, "PQmblenBounded",
                                   "invalid encoding name");
        return 2;
    }

    // Same, but not more than the distance to the end of string s
    lua_pushinteger(L, PQmblenBounded(s, encoding));
    return 1;
}

static int mblen_lua(lua_State *L)
{
    const char *s = lauxh_checkstring(L, 1);
    int encoding  = pg_char_to_encoding(lauxh_checkstring(L, 2));

    if (encoding == -1) {
        lua_pushnil(L);
        lua_errno_new_with_message(L, EINVAL, "PQmblen",
                                   "invalid encoding name");
        return 2;
    }
    // Determine length of multibyte encoded char at *s
    lua_pushinteger(L, PQmblen(s, encoding));
    return 1;
}

static int lib_version_lua(lua_State *L)
{
    lua_pushinteger(L, PQlibVersion());
    return 1;
}

/* Quoting strings before inclusion in queries. */

/* These forms are deprecated! */
// extern size_t PQescapeString(char *to, const char *from, size_t length);
// extern unsigned char *PQescapeBytea(const unsigned char *from,
//                                     size_t from_length, size_t *to_length);

static int unescape_bytea_lua(lua_State *L)
{
    const char *strtext = lauxh_checkstring(L, 1);
    size_t len          = 0;
    unsigned char *to   = NULL;

    errno = 0;
    to    = PQunescapeBytea((const unsigned char *)strtext, &len);
    lua_settop(L, 0);

    if (to) {
        lua_pushlstring(L, (const char *)to, len);
        PQfreemem((void *)to);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "PQunescapeBytea");
    return 2;
}

static int is_threadsafe_lua(lua_State *L)
{
    lua_pushboolean(L, PQisthreadsafe());
    return 1;
}

static int ping_lua(lua_State *L)
{
    const char *conninfo = lauxh_optstring(L, 1, "");

    switch (PQping(conninfo)) {
    case PQPING_OK:
        // server is accepting connections
        lua_pushstring(L, "ok");
        return 1;
    case PQPING_REJECT:
        // server is alive but rejecting connections
        lua_pushstring(L, "reject");
        return 1;
    case PQPING_NO_RESPONSE:
        // could not establish connection
        lua_pushstring(L, "no_response");
        return 1;
    case PQPING_NO_ATTEMPT:
        // connection not attempted (bad params)
        lua_pushstring(L, "no_attempt");
        return 1;
    default:
        lua_pushstring(L, "unknown PGPing result");
        return 1;
    }
}

static int conninfo_parse_lua(lua_State *L)
{
    const char *conninfo      = lauxh_checkstring(L, 1);
    char *errmsg              = NULL;
    PQconninfoOption *options = NULL;

    errno   = 0;
    options = PQconninfoParse(conninfo, &errmsg);
    lua_settop(L, 0);

    if (options) {
        lpg_push_conninfo_options(L, options);
        PQconninfoFree(options);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQconninfoParse", errmsg);
    free(errmsg);
    return 2;
}

static int conninfo_defaults_lua(lua_State *L)
{
    PQconninfoOption *options = NULL;

    errno   = 0;
    options = PQconndefaults();
    lua_settop(L, 0);

    if (options) {
        lpg_push_conninfo_options(L, options);
        PQconninfoFree(options);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "PQconndefaults");
    return 2;
}

LUALIB_API int luaopen_postgres_misc(lua_State *L)
{
    lua_errno_loadlib(L);

    // create module table
    lua_createtable(L, 0, 13);
    lauxh_pushfn2tbl(L, "conninfo_defaults", conninfo_defaults_lua);
    lauxh_pushfn2tbl(L, "conninfo_parse", conninfo_parse_lua);
    lauxh_pushfn2tbl(L, "ping", ping_lua);
    lauxh_pushfn2tbl(L, "is_threadsafe", is_threadsafe_lua);
    lauxh_pushfn2tbl(L, "unescape_bytea", unescape_bytea_lua);
    lauxh_pushfn2tbl(L, "lib_version", lib_version_lua);
    lauxh_pushfn2tbl(L, "mblen", mblen_lua);
    lauxh_pushfn2tbl(L, "mblen_bounded", mblen_bounded_lua);
    lauxh_pushfn2tbl(L, "dsplen", dsplen_lua);
    lauxh_pushfn2tbl(L, "env2encoding", env2encoding_lua);
    lauxh_pushfn2tbl(L, "encrypt_password", encrypt_password_lua);
    lauxh_pushfn2tbl(L, "char_to_encoding", char_to_encoding_lua);
    lauxh_pushfn2tbl(L, "encoding_to_char", encoding_to_char_lua);
    lauxh_pushfn2tbl(L, "valid_server_encoding_id",
                     valid_server_encoding_id_lua);

    // Interface for multiple-result or asynchronous queries
    lauxh_pushint2tbl(L, "PQ_QUERY_PARAM_MAX_LIMIT", PQ_QUERY_PARAM_MAX_LIMIT);

    return 1;
}
