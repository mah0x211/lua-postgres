/**
 *  Copyright (C) 2023 Masatoshi Fukunaga
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

#include <arpa/inet.h>
// lua
#include "lauxhlib.h"

static int ntohl_lua(lua_State *L)
{
    size_t len      = 0;
    const char *str = lauxh_checklstring(L, 1, &len);
    int32_t n       = 0;

    // check length
    if (len < sizeof(int32_t)) {
        return lauxh_argerror(L, 1, "invalid length");
    }

    memcpy(&n, str, sizeof(int32_t));
    n = ntohl(n);
    lua_pushinteger(L, n);
    return 1;
}

LUALIB_API int luaopen_postgres_ntohl(lua_State *L)
{
    lua_pushcfunction(L, ntohl_lua);
    return 1;
}
