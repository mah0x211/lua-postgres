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

static inline pgconn_t *checkself(lua_State *L)
{
    pgconn_t *c = luaL_checkudata(L, 1, PGCONN_MT);
    if (!c->conn) {
        luaL_error(L, "attempt to use a freed object");
    }
    return c;
}

static int encrypt_password_conn_lua(lua_State *L)
{
    PGconn *conn          = pgconn_check(L);
    const char *passwd    = lauxh_checkstring(L, 2);
    const char *user      = lauxh_checkstring(L, 3);
    const char *algorithm = lauxh_optstring(L, 4, NULL);
    char *res             = NULL;

    errno = 0;
    res   = PQencryptPasswordConn(conn, passwd, user, algorithm);
    if (res) {
        lua_pushstring(L, res);
        PQfreemem(res);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQencryptPasswordConn",
                               PQerrorMessage(conn));
    return 2;
}

static int escape_bytea_conn_lua(lua_State *L)
{
    PGconn *conn      = pgconn_check(L);
    size_t len        = 0;
    const char *from  = lauxh_checklstring(L, 2, &len);
    unsigned char *to = NULL;

    errno = 0;
    to    = PQescapeByteaConn(conn, (const unsigned char *)from, len, &len);
    if (to) {
        lua_pushlstring(L, (const char *)to, len);
        PQfreemem((void *)to);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQescapeByteaConn",
                               PQerrorMessage(conn));
    return 2;
}

static int escape_identifier_lua(lua_State *L)
{
    PGconn *conn    = pgconn_check(L);
    size_t len      = 0;
    const char *str = lauxh_checklstring(L, 2, &len);
    char *to        = NULL;

    errno = 0;
    to    = PQescapeIdentifier(conn, str, len);
    if (to) {
        lua_pushstring(L, to);
        PQfreemem(to);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQescapeIdentifier",
                               PQerrorMessage(conn));
    return 2;
}

static int escape_literal_lua(lua_State *L)
{
    PGconn *conn    = pgconn_check(L);
    size_t len      = 0;
    const char *str = lauxh_checklstring(L, 2, &len);
    char *to        = NULL;

    errno = 0;
    to    = PQescapeLiteral(conn, str, len);
    if (to) {
        lua_pushstring(L, to);
        PQfreemem(to);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQescapeLiteral",
                               PQerrorMessage(conn));
    return 2;
}

static int escape_string_conn_lua(lua_State *L)
{
    PGconn *conn     = pgconn_check(L);
    size_t len       = 0;
    const char *from = lauxh_checklstring(L, 2, &len);
    // For safety the buffer at "to" must be at least 2*length + 1 bytes long.
    // A terminating NUL character is added to the output string, whether the
    // input is NUL-terminated or not.
    int err          = 0;
    char *to         = lua_newuserdata(L, len * 2 + 1);
    size_t to_len    = 0;

    errno  = 0;
    to_len = PQescapeStringConn(conn, to, from, len, &err);
    if (err) {
        if (errno == 0) {
            errno = ECANCELED;
        }
        lua_pushnil(L);
        lua_errno_new_with_message(L, errno, "PQescapeStringConn",
                                   PQerrorMessage(conn));
        return 2;
    }

    lua_pushlstring(L, to, to_len);
    return 1;
}

/* Create and manipulate PGresults */
static int make_empty_result_lua(lua_State *L)
{
    PGconn *conn    = pgconn_check(L);
    int status      = lpg_check_pg_exec_status_type(L, 2, "command_ok");
    pgresult_t *res = lua_newuserdata(L, sizeof(pgresult_t));

    errno       = 0;
    res->result = PQmakeEmptyPGresult(conn, status);
    if (res->result) {
        res->ref_conn     = lauxh_refat(L, 1);
        res->is_allocated = LPGRESULT_IS_ALLOCATED;
        lauxh_setmetatable(L, PGRESULT_MT);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQmakeEmptyPGresult",
                               PQerrorMessage(conn));
    return 2;
}

/* Describe prepared statements and portals */
// extern PGresult *PQdescribePrepared(PGconn *conn, const char *stmt);
// extern PGresult *PQdescribePortal(PGconn *conn, const char *portal);
// extern int	PQsendDescribePrepared(PGconn *conn, const char *stmt);
// extern int	PQsendDescribePortal(PGconn *conn, const char *portal);

static int flush_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);

    errno = 0;
    switch (PQflush(conn)) {
    case 0:
        // done
        lua_pushboolean(L, 1);
        return 1;

    case 1:
        // should try again
        lua_pushboolean(L, 0);
        lua_pushnil(L);
        lua_pushboolean(L, 1);
        return 3;

    default:
        if (errno == 0) {
            errno = ECANCELED;
        }
        lua_pushboolean(L, 0);
        lua_errno_new_with_message(L, errno, "PQflush", PQerrorMessage(conn));
        return 2;
    }
}

static int is_nonblocking_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushboolean(L, PQisnonblocking(conn));
    return 1;
}

static int set_nonblocking_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    int enabled  = lauxh_checkboolean(L, 2);

    errno = 0;
    if (PQsetnonblocking(conn, enabled) != -1) {
        lua_pushboolean(L, 1);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_errno_new_with_message(L, errno, "PQsetnonblocking",
                               PQerrorMessage(conn));
    return 2;
}

/* Deprecated routines for copy in/out */
// extern int	PQgetline(PGconn *conn, char *string, int length);
// extern int	PQputline(PGconn *conn, const char *string);
// extern int	PQgetlineAsync(PGconn *conn, char *buffer, int bufsize);
// extern int	PQputnbytes(PGconn *conn, const char *buffer, int nbytes);
// extern int	PQendcopy(PGconn *conn);

static int get_copy_data_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    int async    = lauxh_optboolean(L, 2, 0);
    char *buffer = NULL;
    int nbytes   = 0;

    errno  = 0;
    nbytes = PQgetCopyData(conn, &buffer, async);
    switch (nbytes) {
    case -2:
        lua_pushnil(L);
        if (errno == 0) {
            errno = ECANCELED;
        }
        lua_errno_new_with_message(L, errno, "PQgetCopyData",
                                   PQerrorMessage(conn));
        return 2;

    case -1:
        // completed
        return 0;

    case 0:
        // in-progress
        lua_pushnil(L);
        lua_pushnil(L);
        lua_pushboolean(L, 1);
        return 3;

    default:
        lua_pushlstring(L, buffer, nbytes);
        PQfreemem(buffer);
        return 1;
    }
}

static int put_copy_end_lua(lua_State *L)
{
    PGconn *conn         = pgconn_check(L);
    const char *errormsg = lauxh_optstring(L, 2, NULL);

    errno = 0;
    switch (PQputCopyEnd(conn, errormsg)) {
    case -1:
        lua_pushboolean(L, 0);
        if (errno == 0) {
            errno = ECANCELED;
        }
        lua_errno_new_with_message(L, errno, "PQputCopyEnd",
                                   PQerrorMessage(conn));
        return 2;

    case 0:
        // no buffer space available, should try again
        lua_pushboolean(L, 0);
        lua_pushnil(L);
        lua_pushboolean(L, 1);
        return 3;

    default:
        lua_pushboolean(L, 1);
        return 1;
    }
}

static int put_copy_data_lua(lua_State *L)
{
    PGconn *conn       = pgconn_check(L);
    size_t nbytes      = 0;
    const char *buffer = lauxh_checklstring(L, 2, &nbytes);

    errno = 0;
    switch (PQputCopyData(conn, buffer, nbytes)) {
    case -1:
        lua_pushboolean(L, 0);
        if (errno == 0) {
            errno = ECANCELED;
        }
        lua_errno_new_with_message(L, errno, "PQputCopyData",
                                   PQerrorMessage(conn));
        return 2;

    case 0:
        // no buffer space available, should try again
        lua_pushboolean(L, 0);
        lua_pushnil(L);
        lua_pushboolean(L, 1);
        return 3;

    default:
        // queued
        lua_pushboolean(L, 1);
        return 1;
    }
}

static int notifies_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    // remove unused arguments
    lua_settop(L, 1);

    errno = 0;
    if (PQconsumeInput(conn)) {
        PGnotify *notify = PQnotifies(conn);
        if (notify) {
            lua_createtable(L, 0, 3);
            lauxh_pushstr2tbl(L, "relname", notify->relname);
            lauxh_pushstr2tbl(L, "extra", notify->extra);
            lauxh_pushint2tbl(L, "be_pid", notify->be_pid);
            PQfreemem(notify);
            return 1;
        }
        lua_pushnil(L);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQconsumeInput",
                               PQerrorMessage(conn));
    return 2;
}

static int send_flush_request_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);

    errno = 0;
    if (PQsendFlushRequest(conn)) {
        lua_pushboolean(L, 1);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_errno_new_with_message(L, errno, "PQsendFlushRequest",
                               PQerrorMessage(conn));
    return 2;
}

static int pipeline_sync_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);

    errno = 0;
    if (PQpipelineSync(conn)) {
        lua_pushboolean(L, 1);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_errno_new_with_message(L, errno, "PQpipelineSync",
                               PQerrorMessage(conn));
    return 2;
}

static int exit_pipeline_mode_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);

    errno = 0;
    if (PQexitPipelineMode(conn)) {
        lua_pushboolean(L, 1);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_errno_new_with_message(L, errno, "PQexitPipelineMode",
                               PQerrorMessage(conn));
    return 2;
}

static int enter_pipeline_mode_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);

    errno = 0;
    if (PQenterPipelineMode(conn)) {
        lua_pushboolean(L, 1);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_errno_new_with_message(L, errno, "PQenterPipelineMode",
                               PQerrorMessage(conn));
    return 2;
}

static int consume_input_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);

    errno = 0;
    if (PQconsumeInput(conn)) {
        lua_pushboolean(L, 1);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_errno_new_with_message(L, errno, "PQconsumeInput",
                               PQerrorMessage(conn));
    return 2;
}

static int is_busy_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);

RETRY:
    errno = 0;
    if (PQconsumeInput(conn)) {
        /**
         * NOTE: In the manual, you should call PQisBusy after PQconsumeInput
         * for asynchronous mode. But, this combination does not work well on
         * the edge-triggered mode.
         *
         * This combination will result in a busy status even if the errno is
         * not set to EAGAIN or EWOULDBLOCK on a socket read error. Thus, even
         * if the socket is monitored with epoll or kqueue, no event will be
         * fired.
         */
        int call_again = errno == EAGAIN || errno == EWOULDBLOCK;
        if (!PQisBusy(conn)) {
            // it can read result
            lua_pushboolean(L, 0);
        } else if (call_again) {
            // it should call PQconsumeInput again after polling the socket
            lua_pushboolean(L, 1);
        } else {
            // it should call PQconsumeInput again while errno is set to EAGAIN
            // or EWOULDBLOCK
            goto RETRY;
        }
        return 1;

    } else if (errno == 0) {
        errno = ECANCELED;
    }
    // got error
    lua_pushboolean(L, 0);
    lua_errno_new_with_message(L, errno, "PQconsumeInput",
                               PQerrorMessage(conn));
    return 2;
}

static int get_result_lua(lua_State *L)
{
    PGconn *conn  = pgconn_check(L);
    pgresult_t *r = lua_newuserdata(L, sizeof(pgresult_t));
    char *errmsg  = NULL;

    errno     = 0;
    r->result = PQgetResult(conn);
    if (r->result) {
        r->ref_conn     = lauxh_refat(L, 1);
        r->is_allocated = LPGRESULT_IS_ALLOCATED;
        lauxh_setmetatable(L, PGRESULT_MT);
        return 1;
    }

    errmsg = PQerrorMessage(conn);
    if (errmsg && *errmsg) {
        // got non-empty error message
        lua_pushnil(L);
        if (errno == 0) {
            errno = ECANCELED;
        }
        lua_errno_new_with_message(L, errno, "PQgetResult", errmsg);
        return 2;
    }
    return 0;
}

static int set_single_row_mode_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushboolean(L, PQsetSingleRowMode(conn));
    return 1;
}

static inline const char *param2string(lua_State *L, int idx)
{
    int type = lua_type(L, idx);

    if (idx < 0) {
        idx = lua_gettop(L) + idx + 1;
    }

    if (type != LUA_TSTRING) {
        switch (type) {
        case LUA_TNONE:
        case LUA_TNIL:
            return NULL;

        case LUA_TBOOLEAN:
            if (lua_toboolean(L, idx)) {
                lua_pushliteral(L, "TRUE");
            } else {
                lua_pushliteral(L, "FALSE");
            }
            break;

        case LUA_TNUMBER:
            lua_pushstring(L, lua_tostring(L, idx));
            break;

        // case LUA_TTHREAD:
        // case LUA_TLIGHTUSERDATA:
        // case LUA_TTABLE:
        // case LUA_TFUNCTION:
        // case LUA_TUSERDATA:
        // case LUA_TTHREAD:
        default:
            lauxh_argerror(L, idx, "<%s> param is not supported",
                           luaL_typename(L, idx));
        }
        lua_replace(L, idx);
    }
    return lua_tostring(L, idx);
}

static int send_query_params_lua(lua_State *L)
{
    int nparams         = lua_gettop(L) - 2;
    PGconn *conn        = pgconn_check(L);
    const char *command = lauxh_checkstring(L, 2);
    const char **params = NULL;

    if (nparams) {
        params = lua_newuserdata(L, sizeof(char *) * nparams);
        for (int i = 0, j = 3; i < nparams; i++, j++) {
            params[i] = param2string(L, j);
        }
    }

    errno = 0;
    if (PQsendQueryParams(conn, command, nparams, NULL, params, NULL, NULL,
                          0)) {
        lua_pushboolean(L, 1);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_errno_new_with_message(L, errno, "PQsendQueryParams",
                               PQerrorMessage(conn));
    return 2;
}

static int send_query_lua(lua_State *L)
{
    PGconn *conn      = pgconn_check(L);
    const char *query = lauxh_checkstring(L, 2);

    errno = 0;
    if (PQsendQuery(conn, query)) {
        lua_pushboolean(L, 1);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_errno_new_with_message(L, errno, "PQsendQuery", PQerrorMessage(conn));
    return 2;
}

static int exec_prepare_lua(lua_State *L)
{
    int nparams         = lua_gettop(L) - 2;
    PGconn *conn        = pgconn_check(L);
    const char *name    = lauxh_checkstring(L, 2);
    const char **params = NULL;
    pgresult_t *res     = NULL;

    if (nparams) {
        params = lua_newuserdata(L, sizeof(char *) * nparams);
        for (int i = 0, j = 3; i < nparams; i++, j++) {
            params[i] = param2string(L, j);
        }
    }

    res         = lua_newuserdata(L, sizeof(pgresult_t));
    errno       = 0;
    res->result = PQexecPrepared(conn, name, nparams, params, NULL, NULL, 0);
    if (res->result) {
        res->ref_conn     = lauxh_refat(L, 1);
        res->is_allocated = LPGRESULT_IS_ALLOCATED;
        lauxh_setmetatable(L, PGRESULT_MT);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQexecPrepared",
                               PQerrorMessage(conn));
    return 2;
}

static int prepare_lua(lua_State *L)
{
    int nparams       = lua_gettop(L) - 3;
    PGconn *conn      = pgconn_check(L);
    const char *name  = lauxh_checkstring(L, 2);
    const char *query = lauxh_checkstring(L, 3);
    Oid *param_types  = NULL;
    pgresult_t *res   = NULL;

    if (nparams) {
        param_types = lua_newuserdata(L, sizeof(Oid) * nparams);
        for (int i = 0, j = 3; i < nparams; i++, j++) {
            param_types[i] = lauxh_checkuinteger(L, j);
        }
    }

    res         = lua_newuserdata(L, sizeof(pgresult_t));
    errno       = 0;
    res->result = PQprepare(conn, name, query, nparams, param_types);
    if (res->result) {
        res->ref_conn     = lauxh_refat(L, 1);
        res->is_allocated = LPGRESULT_IS_ALLOCATED;
        lauxh_setmetatable(L, PGRESULT_MT);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQprepare", PQerrorMessage(conn));
    return 2;
}

static int exec_params_lua(lua_State *L)
{
    int nparams         = lua_gettop(L) - 2;
    PGconn *conn        = pgconn_check(L);
    const char *command = lauxh_checkstring(L, 2);
    const char **params = NULL;
    pgresult_t *res     = NULL;

    if (nparams) {
        params = lua_newuserdata(L, sizeof(char *) * nparams);
        for (int i = 0, j = 3; i < nparams; i++, j++) {
            params[i] = param2string(L, j);
        }
    }

    res   = lua_newuserdata(L, sizeof(pgresult_t));
    errno = 0;
    res->result =
        PQexecParams(conn, command, nparams, NULL, params, NULL, NULL, 0);
    if (res->result) {
        res->ref_conn     = lauxh_refat(L, 1);
        res->is_allocated = LPGRESULT_IS_ALLOCATED;
        lauxh_setmetatable(L, PGRESULT_MT);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQexecParams", PQerrorMessage(conn));
    return 2;
}

static int exec_lua(lua_State *L)
{
    PGconn *conn        = pgconn_check(L);
    const char *command = lauxh_checkstring(L, 2);
    pgresult_t *res     = lua_newuserdata(L, sizeof(pgresult_t));

    errno       = 0;
    res->result = PQexec(conn, command);
    if (res->result) {
        res->ref_conn     = lauxh_refat(L, 1);
        res->is_allocated = LPGRESULT_IS_ALLOCATED;
        lauxh_setmetatable(L, PGRESULT_MT);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new_with_message(L, errno, "PQexec", PQerrorMessage(conn));
    return 2;
}

static int set_trace_flags_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    int flags    = 0;

    for (int i = 2; i <= lua_gettop(L); i++) {
        const char *flg = luaL_checkstring(L, i);
        if (strcmp(flg, "suppress_timestamps") == 0) {
            flags |= PQTRACE_SUPPRESS_TIMESTAMPS;
        } else if (strcmp(flg, "regress_mode") == 0) {
            flags |= PQTRACE_REGRESS_MODE;
        } else {
            return luaL_error(L, "invalid trace flag: %s", flg);
        }
    }
    PQsetTraceFlags(conn, flags);
    return 0;
}

static int untrace_lua(lua_State *L)
{
    pgconn_t *c = checkself(L);

    PQuntrace(c->conn);
    if (c->trace_ref == LUA_NOREF) {
        lua_pushnil(L);
    } else {
        lauxh_pushref(L, c->trace_ref);
        c->trace_ref = lauxh_unref(L, c->trace_ref);
    }
    return 1;
}

static int trace_lua(lua_State *L)
{
    pgconn_t *c      = checkself(L);
    FILE *debug_port = lauxh_checkfile(L, 2);

    // remove old file
    untrace_lua(L);
    // set new file
    c->trace_ref = lauxh_refat(L, 2);
    PQtrace(c->conn, debug_port);

    return 1;
}

static int call_notice_receiver_lua(lua_State *L)
{
    pgconn_t *c = checkself(L);

    luaL_checkudata(L, 2, PGRESULT_MT);
    lua_settop(L, 2);
    if (c->notice_recv_ref == LUA_NOREF) {
        lua_pushboolean(L, 0);
        return 1;
    }

    // call closure
    lauxh_pushref(L, c->notice_recv_ref);
    lua_insert(L, 2);
    lua_call(L, 1, 0);
    lua_pushboolean(L, 1);
    return 1;
}

static int call_notice_processor_lua(lua_State *L)
{
    pgconn_t *c = checkself(L);

    luaL_checktype(L, 2, LUA_TSTRING);
    lua_settop(L, 2);
    if (c->notice_proc_ref == LUA_NOREF) {
        lua_pushboolean(L, 0);
        return 1;
    }

    // call closure
    lauxh_pushref(L, c->notice_proc_ref);
    lua_insert(L, 2);
    lua_call(L, 1, 0);
    lua_pushboolean(L, 1);
    return 1;
}

static int notice_closure(lua_State *L)
{
    int narg = lua_gettop(L);
    int farg = 0;

    // get number of arguments
    lua_pushvalue(L, lua_upvalueindex(1));
    farg = lua_tointeger(L, -1);
    lua_pop(L, 1);

    // push function
    luaL_checkstack(L, farg + narg, "failed to call the notice function");
    lua_pushvalue(L, lua_upvalueindex(2));
    if (narg) {
        lua_insert(L, 1);
    }
    // push upvalues to function arguments
    for (int i = 1; i <= farg; i++) {
        lua_pushvalue(L, lua_upvalueindex(2 + i));
        if (narg) {
            lua_insert(L, 1 + i);
        }
    }
    lua_call(L, farg + narg, 0);

    return 0;
}

static inline void push_notice_closure(lua_State *L)
{
    int argc = lua_gettop(L) - 2;

    // create closure function
    luaL_checktype(L, 2, LUA_TFUNCTION);
    // The first argument sets the number of arguments, the second sets the lua
    // function, and the rest are used as function arguments.
    lua_pushinteger(L, argc);
    lua_insert(L, 2);
    lua_pushcclosure(L, notice_closure, 2 + argc);
}

#define set_notice_closure(L, type, register_fn)                               \
    do {                                                                       \
        pgconn_t *c = checkself((L));                                          \
        /* release old reference */                                            \
        if (c->notice_##type##_ref != LUA_NOREF) {                             \
            c->notice_##type##_ref = lauxh_unref((L), c->notice_##type##_ref); \
        }                                                                      \
                                                                               \
        if (!lua_isnoneornil((L), 2)) {                                        \
            /* push closure function */                                        \
            push_notice_closure((L));                                          \
            c->notice_##type##_ref = lauxh_ref((L));                           \
            if (c->default_##type == NULL) {                                   \
                /* set custom notice function */                               \
                c->default_##type = register_fn(c->conn, notice_##type, c);    \
            }                                                                  \
        } else if (c->default_##type) {                                        \
            /* set default notice function */                                  \
            register_fn(c->conn, c->default_##type, NULL);                     \
            c->default_##type = NULL;                                          \
        }                                                                      \
    } while (0)

static void notice_recv(void *arg, const PGresult *res)
{
    pgconn_t *c   = (pgconn_t *)arg;
    pgresult_t *r = NULL;

    // call closure
    lauxh_pushref(c->L, c->notice_recv_ref);
    r               = lua_newuserdata(c->L, sizeof(PGresult *));
    r->ref_conn     = lauxh_refat(c->L, 1);
    r->result       = (PGresult *)res;
    r->is_allocated = LPGRESULT_IS_NOT_ALLOCATED;
    lauxh_setmetatable(c->L, PGRESULT_MT);
    lua_call(c->L, 1, 0);
}

static int set_notice_receiver_lua(lua_State *L)
{
    set_notice_closure(L, recv, PQsetNoticeReceiver);
    return 0;
}

static void notice_proc(void *arg, const char *message)
{
    pgconn_t *c = (pgconn_t *)arg;

    // call closure
    lauxh_pushref(c->L, c->notice_proc_ref);
    lua_pushstring(c->L, message);
    lua_call(c->L, 1, 0);
}

static int set_notice_processor_lua(lua_State *L)
{
    set_notice_closure(L, proc, PQsetNoticeProcessor);
    return 0;
}

static int set_error_context_visibility_lua(lua_State *L)
{
    PGconn *conn           = pgconn_check(L);
    int context_visibility = lpg_check_pg_context_visibility(L, 2, "errors");

    // set context visibility mode and return old mode
    context_visibility = PQsetErrorContextVisibility(conn, context_visibility);
    lua_pushstring(L, lpg_pg_context_visibility_string(context_visibility));
    return 1;
}

static int set_error_verbosity_lua(lua_State *L)
{
    PGconn *conn  = pgconn_check(L);
    int verbosity = lpg_check_pg_verbosity(L, 2, "default");

    // set verbosity mode and return old mode
    verbosity = PQsetErrorVerbosity(conn, verbosity);
    lua_pushstring(L, lpg_pg_verbosity_string(verbosity));
    return 1;
}

static int ssl_attribute_names_lua(lua_State *L)
{
    PGconn *conn             = pgconn_check(L);
    const char *const *names = PQsslAttributeNames(conn);
    int i                    = 1;

    lua_newtable(L);
    while (*names) {
        lauxh_pushstr2arr(L, i++, *names);
        names++;
    }

    return 1;
}

static int ssl_attribute_lua(lua_State *L)
{
    PGconn *conn               = pgconn_check(L);
    const char *attribute_name = lauxh_checkstring(L, 2);
    lua_pushstring(L, PQsslAttribute(conn, attribute_name));
    return 1;
}

static int ssl_in_use_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushboolean(L, PQsslInUse(conn));
    return 1;
}

static int set_client_encoding_lua(lua_State *L)
{
    PGconn *conn         = pgconn_check(L);
    const char *encoding = lauxh_checkstring(L, 2);

    errno = 0;
    if (PQsetClientEncoding(conn, encoding) == 0) {
        lua_pushboolean(L, 1);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushboolean(L, 0);
    lua_errno_new_with_message(L, errno, "PQsetClientEncoding",
                               PQerrorMessage(conn));
    return 2;
}

static int client_encoding_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushstring(L, pg_encoding_to_char(PQclientEncoding(conn)));
    return 1;
}

static int connection_used_password_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushboolean(L, PQconnectionUsedPassword(conn));
    return 1;
}

static int connection_needs_password_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushboolean(L, PQconnectionNeedsPassword(conn));
    return 1;
}

static int pipeline_status_lua(lua_State *L)
{
    PGconn *conn            = pgconn_check(L);
    PGpipelineStatus status = PQpipelineStatus(conn);

    switch (status) {
    case PQ_PIPELINE_OFF:
        lua_pushliteral(L, "off");
        return 1;
    case PQ_PIPELINE_ON:
        lua_pushliteral(L, "on");
        return 1;
    case PQ_PIPELINE_ABORTED:
        lua_pushliteral(L, "aborted");
        return 1;

    default:
        lua_pushfstring(L, "unknown PGpipelineStatus: %d", status);
        return 1;
    }
}

static int backend_pid_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushinteger(L, PQbackendPID(conn));
    return 1;
}

static int socket_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushinteger(L, PQsocket(conn));
    return 1;
}

static int error_message_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    char *err    = PQerrorMessage(conn);

    if (err && *err) {
        // got non-empty error message
        lua_pushstring(L, err);
        return 1;
    }
    return 0;
}

static int server_version_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushinteger(L, PQserverVersion(conn));
    return 1;
}

static int protocol_version_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushinteger(L, PQprotocolVersion(conn));
    return 1;
}

static int parameter_status_lua(lua_State *L)
{
    PGconn *conn          = pgconn_check(L);
    const char *paramName = lauxh_checkstring(L, 2);
    lua_pushstring(L, PQparameterStatus(conn, paramName));
    return 1;
}

static int transaction_status_lua(lua_State *L)
{
    PGconn *conn                   = pgconn_check(L);
    PGTransactionStatusType status = PQtransactionStatus(conn);

    switch (status) {
    case PQTRANS_IDLE:
        lua_pushliteral(L, "idle");
        return 1;
    case PQTRANS_ACTIVE:
        lua_pushliteral(L, "active");
        return 1;
    case PQTRANS_INTRANS:
        lua_pushliteral(L, "intrans");
        return 1;
    case PQTRANS_INERROR:
        lua_pushliteral(L, "inerror");
        return 1;
    case PQTRANS_UNKNOWN:
        lua_pushliteral(L, "unknown");
        return 1;
    }
    lua_pushfstring(L, "unknown PGTransactionStatusType: %d", status);
    return 1;
}

static int status_lua(lua_State *L)
{
    PGconn *conn          = pgconn_check(L);
    ConnStatusType status = PQstatus(conn);

    switch (status) {
    case CONNECTION_OK:
        lua_pushliteral(L, "ok");
        return 1;
    case CONNECTION_BAD:
        lua_pushliteral(L, "bad");
        return 1;

        // the following are only valid in non-blocking mode

    case CONNECTION_STARTED: // waiting for connection to be made.
        lua_pushliteral(L, "started");
        return 1;

    case CONNECTION_MADE: // connection OK; waiting to send.
        lua_pushliteral(L, "made");
        return 1;

    case CONNECTION_AWAITING_RESPONSE: // waiting for a response from the
                                       // postmaster
        lua_pushliteral(L, "awaiting_response");
        return 1;

    case CONNECTION_AUTH_OK: // received authentication; waiting for backend
                             // startup.
        lua_pushliteral(L, "auth_ok");
        return 1;

    case CONNECTION_SETENV: // this state is no longer used.
        lua_pushliteral(L, "setenv");
        return 1;

    case CONNECTION_SSL_STARTUP: // negotiating SSL.
        lua_pushliteral(L, "ssl_startup");
        return 1;

    case CONNECTION_NEEDED: // internal state: connect() needed
        lua_pushliteral(L, "needed");
        return 1;

    case CONNECTION_CHECK_WRITABLE: // checking if session is read-write
        lua_pushliteral(L, "check_writable");
        return 1;

    case CONNECTION_CONSUME: // consuming any extra messages
        lua_pushliteral(L, "consume");
        return 1;

    case CONNECTION_GSS_STARTUP: // negotiating GSSAPI
        lua_pushliteral(L, "gss_startup");
        return 1;

    case CONNECTION_CHECK_TARGET: // checking target server properties
        lua_pushliteral(L, "check_target");
        return 1;

    case CONNECTION_CHECK_STANDBY: // checking if server is in standby mode
        lua_pushliteral(L, "check_standby");
        return 1;

    default:
        lua_pushfstring(L, "unknown ConnStatusType: %d", status);
        return 1;
    }
}

static int options_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushstring(L, PQoptions(conn));
    return 1;
}

static int port_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushstring(L, PQport(conn));
    return 1;
}

static int hostaddr_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushstring(L, PQhostaddr(conn));
    return 1;
}

static int host_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushstring(L, PQhost(conn));
    return 1;
}

static int pass_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushstring(L, PQpass(conn));
    return 1;
}

static int user_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushstring(L, PQuser(conn));
    return 1;
}

static int db_lua(lua_State *L)
{
    PGconn *conn = pgconn_check(L);
    lua_pushstring(L, PQdb(conn));
    return 1;
}

static int get_cancel_lua(lua_State *L)
{
    PGconn *conn      = pgconn_check(L);
    PGcancel **cancel = NULL;
    lua_settop(L, 1);

    errno   = 0;
    cancel  = lua_newuserdata(L, sizeof(PGcancel *));
    *cancel = PQgetCancel(conn);
    if (*cancel) {
        lauxh_setmetatable(L, PGCANCEL_MT);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    lua_pushnil(L);
    lua_errno_new(L, errno, "PQgetCancel");
    return 2;
}

static int connect_poll_lua(lua_State *L)
{
    PostgresPollingStatusType status = PQconnectPoll(pgconn_check(L));

    switch (status) {
    case PGRES_POLLING_FAILED:
        lua_pushliteral(L, "failed");
        return 1;
    case PGRES_POLLING_READING:
        lua_pushliteral(L, "reading");
        return 1;
    case PGRES_POLLING_WRITING:
        lua_pushliteral(L, "writing");
        return 1;
    case PGRES_POLLING_OK:
        lua_pushliteral(L, "ok");
        return 1;
    case PGRES_POLLING_ACTIVE:
        lua_pushliteral(L, "active");
        return 1;
    default:
        lua_pushfstring(L, "unknown PostgresPollingStatusType: %d", status);
        return 1;
    }
}

static int conninfo_lua(lua_State *L)
{
    PQconninfoOption *options = PQconninfo(pgconn_check(L));

    if (options) {
        lpg_push_conninfo_options(L, options);
        PQconninfoFree(options);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "conninfo");
    return 2;
}

static inline int finish(lua_State *L)
{
    pgconn_t *c = luaL_checkudata(L, 1, PGCONN_MT);

    if (c->conn) {
        PQfinish(c->conn);
        c->conn = NULL;
        lauxh_unref(L, c->notice_recv_ref);
        lauxh_unref(L, c->notice_proc_ref);
        lauxh_unref(L, c->trace_ref);
    }

    return 0;
}

static int finish_lua(lua_State *L)
{
    return finish(L);
}

static int gc_lua(lua_State *L)
{
    return finish(L);
}

static int tostring_lua(lua_State *L)
{
    return lpg_tostring_lua(L, PGCONN_MT);
}

static int connect_lua(lua_State *L)
{
    const char *conninfo = lauxh_optstring(L, 1, "");
    int nonblock         = lauxh_optboolean(L, 2, 0);
    pgconn_t *c          = lua_newuserdata(L, sizeof(pgconn_t));

    *c = (pgconn_t){
        .L               = L,
        .notice_recv_ref = LUA_NOREF,
        .notice_proc_ref = LUA_NOREF,
        .trace_ref       = LUA_NOREF,
    };

    if (nonblock) {
        c->conn = PQconnectStart(conninfo);
    } else {
        c->conn = PQconnectdb(conninfo);
    }

    if (c->conn) {
        lauxh_setmetatable(L, PGCONN_MT);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "connect");
    return 2;
}

LUALIB_API int luaopen_postgres_pgconn(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__gc",       gc_lua      },
        {"__tostring", tostring_lua},
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"finish",                       finish_lua                      },
        {"conninfo",                     conninfo_lua                    },
        {"connect_poll",                 connect_poll_lua                },
        {"get_cancel",                   get_cancel_lua                  },
        {"db",                           db_lua                          },
        {"user",                         user_lua                        },
        {"pass",                         pass_lua                        },
        {"host",                         host_lua                        },
        {"hostaddr",                     hostaddr_lua                    },
        {"port",                         port_lua                        },
        {"options",                      options_lua                     },
        {"status",                       status_lua                      },
        {"transaction_status",           transaction_status_lua          },
        {"parameter_status",             parameter_status_lua            },
        {"protocol_version",             protocol_version_lua            },
        {"server_version",               server_version_lua              },
        {"error_message",                error_message_lua               },
        {"socket",                       socket_lua                      },
        {"backend_pid",                  backend_pid_lua                 },
        {"pipeline_status",              pipeline_status_lua             },
        {"connection_needs_password",    connection_needs_password_lua   },
        {"connection_used_password",     connection_used_password_lua    },
        {"client_encoding",              client_encoding_lua             },
        {"set_client_encoding",          set_client_encoding_lua         },
        {"ssl_in_use",                   ssl_in_use_lua                  },
        {"ssl_attribute",                ssl_attribute_lua               },
        {"ssl_attribute_names",          ssl_attribute_names_lua         },
        {"set_error_verbosity",          set_error_verbosity_lua         },
        {"set_error_context_visibility", set_error_context_visibility_lua},
        {"set_notice_processor",         set_notice_processor_lua        },
        {"set_notice_receiver",          set_notice_receiver_lua         },
        {"call_notice_processor",        call_notice_processor_lua       },
        {"call_notice_receiver",         call_notice_receiver_lua        },
        {"trace",                        trace_lua                       },
        {"untrace",                      untrace_lua                     },
        {"set_trace_flags",              set_trace_flags_lua             },
        {"exec",                         exec_lua                        },
        {"exec_params",                  exec_params_lua                 },
        {"prepare",                      prepare_lua                     },
        {"exec_prepare",                 exec_prepare_lua                },
        {"send_query",                   send_query_lua                  },
        {"send_query_params",            send_query_params_lua           },
        {"set_single_row_mode",          set_single_row_mode_lua         },
        {"get_result",                   get_result_lua                  },
        {"is_busy",                      is_busy_lua                     },
        {"consume_input",                consume_input_lua               },
        {"enter_pipeline_mode",          enter_pipeline_mode_lua         },
        {"exit_pipeline_mode",           exit_pipeline_mode_lua          },
        {"pipeline_sync",                pipeline_sync_lua               },
        {"send_flush_request",           send_flush_request_lua          },
        {"notifies",                     notifies_lua                    },
        {"put_copy_data",                put_copy_data_lua               },
        {"put_copy_end",                 put_copy_end_lua                },
        {"get_copy_data",                get_copy_data_lua               },
        {"set_nonblocking",              set_nonblocking_lua             },
        {"is_nonblocking",               is_nonblocking_lua              },
        {"flush",                        flush_lua                       },
        {"make_empty_result",            make_empty_result_lua           },
        {"escape_string_conn",           escape_string_conn_lua          },
        {"escape_literal",               escape_literal_lua              },
        {"escape_identifier",            escape_identifier_lua           },
        {"escape_bytea_conn",            escape_bytea_conn_lua           },
        {"encrypt_password_conn",        encrypt_password_conn_lua       },
        {NULL,                           NULL                            }
    };

    lua_errno_loadlib(L);
    init_postgres_pgresult(L);
    init_postgres_pgcancel(L);
    lpg_register_mt(L, PGCONN_MT, mmethod, method);
    lua_pop(L, 1);

    // create module table
    lua_pushcfunction(L, connect_lua);
    return 1;
}
