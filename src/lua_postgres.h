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

#ifndef lua_postgres_h
#define lua_postgres_h

#include <inttypes.h>
#include <stdlib.h>
#include <string.h>
// libpq
#include <libpq-fe.h>
#include <postgres_ext.h>
// lua
#include <lauxhlib.h>
#include <lua_errno.h>

static inline uintmax_t lpg_str2uint(char *str)
{
    errno = 0;
    if (*str) {
        return strtoumax(str, NULL, 10);
    }
    errno = ERANGE;
    return UINTMAX_MAX;
}

static inline void lpg_push_conninfo_options(lua_State *L,
                                             PQconninfoOption *options)
{
    PQconninfoOption *opt = options;

    lua_newtable(L);
    while (opt->keyword) {
        lua_createtable(L, 0, 6);
        // Fallback environment variable name
        lauxh_pushstr2tbl(L, "envvar", opt->envvar);
        // Fallback compiled in default value
        lauxh_pushstr2tbl(L, "compiled", opt->compiled);
        // Option's current value, or NULL
        lauxh_pushstr2tbl(L, "val", opt->val);
        // Label for field in connect dialog
        lauxh_pushstr2tbl(L, "label", opt->label);
        // Indicates how to display this field in a
        // connect dialog. Values are: "" Display
        // entered value as is "*" Password field -
        // hide value "D"  Debug option - don't show
        // by default
        lauxh_pushstr2tbl(L, "dispchar", opt->dispchar);
        /* Field size in characters for dialog	*/
        lauxh_pushint2tbl(L, "dispsize", opt->dispsize);
        // The keyword of the option
        lua_setfield(L, -2, opt->keyword);
        opt++;
    }
}

static inline int lpg_tostring_lua(lua_State *L, const char *tname)
{
    void *p = luaL_checkudata(L, 1, tname);
    lua_pushfstring(L, "%s: %p", tname, p);
    return 1;
}

static inline void lpg_register_mt(lua_State *L, const char *tname,
                                   struct luaL_Reg mmethod[],
                                   struct luaL_Reg method[])
{
    // create metatable
    luaL_newmetatable(L, tname);
    // metamethods
    for (struct luaL_Reg *ptr = mmethod; ptr->name; ptr++) {
        lauxh_pushfn2tbl(L, ptr->name, ptr->func);
    }
    // methods
    lua_pushstring(L, "__index");
    lua_newtable(L);
    for (struct luaL_Reg *ptr = method; ptr->name; ptr++) {
        lauxh_pushfn2tbl(L, ptr->name, ptr->func);
    }
    lua_rawset(L, -3);
    lua_pop(L, 1);
}

static inline PGVerbosity lpg_check_pg_verbosity(lua_State *L, int idx,
                                                 const char *defval)
{
    static const char *const verbosity[] = {
        "terse", "default", "verbose", "sqlstate", NULL,
    };
    switch (luaL_checkoption(L, idx, defval, verbosity)) {
    case 0:
        return PQERRORS_TERSE;
    case 1:
        return PQERRORS_DEFAULT;
    case 2:
        return PQERRORS_VERBOSE;
    default:
        return PQERRORS_SQLSTATE;
    }
}

static inline char *lpg_pg_verbosity_string(PGVerbosity verbosity)
{
    switch (verbosity) {
    case PQERRORS_TERSE:
        return "terse";
    case PQERRORS_DEFAULT:
        return "default";
    case PQERRORS_VERBOSE:
        return "verbose";
    case PQERRORS_SQLSTATE:
        return "sqlstate";
    default:
        return "unknown PGVerbosity";
    }
}

static inline int lpg_check_pg_context_visibility(lua_State *L, int idx,
                                                  const char *defval)
{
    static const char *const context_visibility[] = {
        "never",
        "errors",
        "always",
        NULL,
    };

    switch (luaL_checkoption(L, idx, defval, context_visibility)) {
    case 0:
        return PQSHOW_CONTEXT_NEVER;
    case 1:
        return PQSHOW_CONTEXT_ERRORS;
    default:
        return PQSHOW_CONTEXT_ALWAYS;
    }
}

static inline char *lpg_pg_context_visibility_string(PGContextVisibility ctx)
{
    switch (ctx) {
    case PQSHOW_CONTEXT_NEVER:
        return "never";
    case PQSHOW_CONTEXT_ERRORS:
        return "error";
    case PQSHOW_CONTEXT_ALWAYS:
        return "always";
    default:
        return "unknown PGContextVisibility";
    }
}

static inline int lpg_check_pg_exec_status_type(lua_State *L, int idx,
                                                const char *defval)
{
    static const char *const exec_status_type[] = {
        "empty_query", "command_ok",   "tuples_ok",      "copy_out",
        "copy_in",     "bad_response", "nonfatal_error", "fatal_error",
        "copy_both",   "single_tuple", "pipeline_sync",  "pipeline_aborted",
        NULL,
    };

    switch (luaL_checkoption(L, idx, defval, exec_status_type)) {
    case 0:
        return PGRES_EMPTY_QUERY;
    case 1:
        return PGRES_COMMAND_OK;
    case 2:
        return PGRES_TUPLES_OK;
    case 3:
        return PGRES_COPY_OUT;
    case 4:
        return PGRES_COPY_IN;
    case 5:
        return PGRES_BAD_RESPONSE;
    case 6:
        return PGRES_NONFATAL_ERROR;
    case 7:
        return PGRES_FATAL_ERROR;
    case 8:
        return PGRES_COPY_BOTH;
    case 9:
        return PGRES_SINGLE_TUPLE;
    case 10:
        return PGRES_PIPELINE_SYNC;
    default:
        return PGRES_PIPELINE_ABORTED;
    }
}

static inline char *lpg_pg_exec_status_type_string(ExecStatusType status)
{
    switch (status) {
    case PGRES_EMPTY_QUERY:
        return "empty_query";
    case PGRES_COMMAND_OK:
        return "command_ok";
    case PGRES_TUPLES_OK:
        return "tuples_ok";
    case PGRES_COPY_OUT:
        return "copy_out";
    case PGRES_COPY_IN:
        return "copy_in";
    case PGRES_BAD_RESPONSE:
        return "bad_response";
    case PGRES_NONFATAL_ERROR:
        return "nonfatal_error";
    case PGRES_FATAL_ERROR:
        return "fatal_error";
    case PGRES_COPY_BOTH:
        return "copy_both";
    case PGRES_SINGLE_TUPLE:
        return "single_tuple";
    case PGRES_PIPELINE_SYNC:
        return "pipeline_sync";
    case PGRES_PIPELINE_ABORTED:
        return "pipeline_aborted";
    default:
        return "unknown ExecStatusType";
    }
}

#define PGCANCEL_MT "postgres.pgcancel"

void init_postgres_pgcancel(lua_State *L);

#define PGRESULT_MT "postgres.pgresult"

typedef struct {
    int ref_conn;
#define LPGRESULT_IS_NOT_ALLOCATED 0x00
#define LPGRESULT_IS_ALLOCATED     0x01
    int is_allocated;
    PGresult *result;
} pgresult_t;

void init_postgres_pgresult(lua_State *L);

static inline PGresult *pgresult_check(lua_State *L)
{
    pgresult_t *r = luaL_checkudata(L, 1, PGRESULT_MT);
    if (!r->result) {
        luaL_error(L, "attempt to use a freed object");
    }
    return r->result;
}

#define PGCONN_MT "postgres.pgconn"

typedef struct {
    lua_State *L;
    int notice_proc_ref;
    int notice_recv_ref;
    int trace_ref;
    PQnoticeProcessor default_proc;
    PQnoticeReceiver default_recv;
    PGconn *conn;
} pgconn_t;

static inline PGconn *pgconn_check(lua_State *L)
{
    pgconn_t *c = luaL_checkudata(L, 1, PGCONN_MT);
    if (!c->conn) {
        luaL_error(L, "attempt to use a freed object");
    }
    return c->conn;
}

#endif
