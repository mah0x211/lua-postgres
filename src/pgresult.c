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

static int param_type_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int param_num       = lauxh_checkinteger(L, 2);
    // get the oid of the parameter's data type
    Oid oid             = PQparamtype(res, param_num);

    if (oid) {
        lua_pushinteger(L, oid);
        return 1;
    }
    return 0;
}

static int nparams_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    lua_pushinteger(L, PQnparams(res));
    return 1;
}

static int get_is_null_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int row             = lauxh_checkpinteger(L, 2) - 1;
    int col             = lauxh_checkpinteger(L, 3) - 1;
    lua_pushboolean(L, PQgetisnull(res, row, col));
    return 1;
}

static int get_length_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int row             = lauxh_checkpinteger(L, 2) - 1;
    int col             = lauxh_checkpinteger(L, 3) - 1;
    lua_pushinteger(L, PQgetlength(res, row, col));
    return 1;
}

static int get_value_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int row             = lauxh_checkpinteger(L, 2) - 1;
    int col             = lauxh_checkpinteger(L, 3) - 1;

    if (PQgetisnull(res, row, col)) {
        lua_pushnil(L);
    } else {
        lua_pushstring(L, PQgetvalue(res, row, col));
    }
    return 1;
}

static int cmd_tuples_lua(lua_State *L)
{
    PGresult *res        = pgresult_check(L);
    uintmax_t cmd_tuples = lpg_str2uint(PQcmdTuples(res));

    if (cmd_tuples != UINTMAX_MAX) {
        lua_pushinteger(L, cmd_tuples);
        return 1;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "PQcmdTuples");
    return 2;
}

static int oid_value_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    // get the oid of the inserted row
    Oid oid             = PQoidValue(res);

    if (oid) {
        lua_pushinteger(L, oid);
        return 1;
    }
    return 0;
}

static int cmd_status_lua(lua_State *L)
{
    PGresult *res = pgresult_check(L);
    // get the command status string of the last command
    lua_pushstring(L, PQcmdStatus(res));
    return 1;
}

static int fmod_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;
    // get the type modifier of the given column
    lua_pushinteger(L, PQfmod(res, col));
    return 1;
}

static int fsize_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;
    // get the size of the data type of the given column
    lua_pushinteger(L, PQfsize(res, col));
    return 1;
}

static int ftype_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;
    // get the oid of the data type of the given column
    Oid oid             = PQftype(res, col);

    if (oid != InvalidOid) {
        lua_pushinteger(L, oid);
        return 1;
    }
    return 0;
}

static int fformat_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;

    switch (PQfformat(res, col)) {
    case 0:
        lua_pushliteral(L, "text");
        return 1;
    case 1:
        lua_pushliteral(L, "binary");
        return 1;
    default:
        lua_pushliteral(L, "unknown");
        return 1;
    }
}

static int ftablecol_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;
    int tblcol          = PQftablecol(res, col);

    if (tblcol) {
        lua_pushinteger(L, tblcol);
        return 1;
    }
    return 0;
}

static int ftable_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;
    // get the table OID column number of the specified column name
    Oid oid             = PQftable(res, col);

    if (oid != InvalidOid) {
        lua_pushinteger(L, oid);
        return 1;
    }
    return 0;
}

static int fnumber_lua(lua_State *L)
{
    const PGresult *res  = pgresult_check(L);
    const char *col_name = lauxh_checkstring(L, 2);
    // get the column number of the specified column name
    int col              = PQfnumber(res, col_name);

    if (col != -1) {
        lua_pushinteger(L, col + 1);
        return 1;
    }
    return 0;
}

static int fname_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int col             = lauxh_checkpinteger(L, 2) - 1;
    // get the column name of the specified column number
    char *name          = PQfname(res, col);

    if (name) {
        lua_pushstring(L, name);
        return 1;
    }
    return 0;
}

static int binary_tuples_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    // whether all column data are binary or not
    lua_pushboolean(L, PQbinaryTuples(res));
    return 1;
}

static int nfields_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    // get number of columns
    lua_pushinteger(L, PQnfields(res));
    return 1;
}

static int ntuples_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    // get number of rows
    lua_pushinteger(L, PQntuples(res));
    return 1;
}

static int error_field_lua(lua_State *L)
{
    static const char *const result_error_fieldcode[] = {
        "severity",
        "severity_nonlocalize",
        "sqlstate",
        "message_primary",
        "message_detail",
        "message_hint",
        "statement_position",
        "internal_position",
        "internal_query",
        "context",
        "schema_name",
        "table_name",
        "column_name",
        "datatype_name",
        "constraint_name",
        "source_file",
        "source_line",
        "source_function",
        NULL,
    };

    const PGresult *res = pgresult_check(L);
    int fieldcode       = luaL_checkoption(L, 2, NULL, result_error_fieldcode);

    switch (fieldcode) {
    case 0:
        fieldcode = PG_DIAG_SEVERITY;
        break;
    case 1:
        fieldcode = PG_DIAG_SEVERITY_NONLOCALIZED;
        break;
    case 2:
        fieldcode = PG_DIAG_SQLSTATE;
        break;
    case 3:
        fieldcode = PG_DIAG_MESSAGE_PRIMARY;
        break;
    case 4:
        fieldcode = PG_DIAG_MESSAGE_DETAIL;
        break;
    case 5:
        fieldcode = PG_DIAG_MESSAGE_HINT;
        break;
    case 6:
        fieldcode = PG_DIAG_STATEMENT_POSITION;
        break;
    case 7:
        fieldcode = PG_DIAG_INTERNAL_POSITION;
        break;
    case 8:
        fieldcode = PG_DIAG_INTERNAL_QUERY;
        break;
    case 9:
        fieldcode = PG_DIAG_CONTEXT;
        break;
    case 10:
        fieldcode = PG_DIAG_SCHEMA_NAME;
        break;
    case 11:
        fieldcode = PG_DIAG_TABLE_NAME;
        break;
    case 12:
        fieldcode = PG_DIAG_COLUMN_NAME;
        break;
    case 13:
        fieldcode = PG_DIAG_DATATYPE_NAME;
        break;
    case 14:
        fieldcode = PG_DIAG_CONSTRAINT_NAME;
        break;
    case 15:
        fieldcode = PG_DIAG_SOURCE_FILE;
        break;
    case 16:
        fieldcode = PG_DIAG_SOURCE_LINE;
        break;
    case 17:
        fieldcode = PG_DIAG_SOURCE_FUNCTION;
        break;
    }

    lua_pushstring(L, PQresultErrorField(res, fieldcode));
    return 1;
}

static int verbose_error_message_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    int verbosity       = lpg_check_pg_verbosity(L, 2, "default");
    int ctx_visibility  = lpg_check_pg_context_visibility(L, 3, "errors");
    char *msg           = NULL;
    lua_settop(L, 1);

    errno = 0;
    msg   = PQresultVerboseErrorMessage(res, verbosity, ctx_visibility);
    if (msg) {
        lua_pushstring(L, msg);
        PQfreemem(msg);
        return 1;
    } else if (errno == 0) {
        errno = ECANCELED;
    }

    // got error
    lua_pushnil(L);
    lua_errno_new(L, errno, "PQresultVerboseErrorMessage");
    return 2;
}

static int error_message_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    char *err           = PQresultErrorMessage(res);

    if (err && *err) {
        lua_pushstring(L, err);
        return 1;
    }
    return 0;
}

static int status_lua(lua_State *L)
{
    const PGresult *res = pgresult_check(L);
    lua_pushstring(L, lpg_pg_exec_status_type_string(PQresultStatus(res)));
    return 1;
}

static int connection_lua(lua_State *L)
{
    pgresult_t *r = luaL_checkudata(L, 1, PGRESULT_MT);
    lauxh_pushref(L, r->ref_conn);
    return 1;
}

static inline int clear(lua_State *L)
{
    pgresult_t *r = luaL_checkudata(L, 1, PGRESULT_MT);

    r->ref_conn = lauxh_unref(L, r->ref_conn);
    if (r->is_allocated && r->result) {
        PQclear(r->result);
        r->result = NULL;
    }

    return 0;
}

static int clear_lua(lua_State *L)
{
    return clear(L);
}

static int gc_lua(lua_State *L)
{
    return clear(L);
}

static int tostring_lua(lua_State *L)
{
    return lpg_tostring_lua(L, PGRESULT_MT);
}

void init_postgres_pgresult(lua_State *L)
{
    struct luaL_Reg mmethod[] = {
        {"__gc",       gc_lua      },
        {"__tostring", tostring_lua},
        {NULL,         NULL        }
    };
    struct luaL_Reg method[] = {
        {"clear",                 clear_lua                },
        {"connection",            connection_lua           },
        {"status",                status_lua               },
        {"error_message",         error_message_lua        },
        {"verbose_error_message", verbose_error_message_lua},
        {"error_field",           error_field_lua          },
        {"ntuples",               ntuples_lua              },
        {"nfields",               nfields_lua              },
        {"binary_tuples",         binary_tuples_lua        },
        {"fname",                 fname_lua                },
        {"fnumber",               fnumber_lua              },
        {"ftable",                ftable_lua               },
        {"ftablecol",             ftablecol_lua            },
        {"fformat",               fformat_lua              },
        {"ftype",                 ftype_lua                },
        {"fsize",                 fsize_lua                },
        {"fmod",                  fmod_lua                 },
        {"cmd_status",            cmd_status_lua           },
        {"oid_value",             oid_value_lua            },
        {"cmd_tuples",            cmd_tuples_lua           },
        {"get_value",             get_value_lua            },
        {"get_length",            get_length_lua           },
        {"get_is_null",           get_is_null_lua          },
        {"nparams",               nparams_lua              },
        {"param_type",            param_type_lua           },
 // {"copy",                  copy_lua                 },
        {NULL,                    NULL                     }
    };
    lpg_register_mt(L, PGRESULT_MT, mmethod, method);
}
