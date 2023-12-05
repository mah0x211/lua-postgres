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

#include "lauxhlib.h"

static int strxor_lua(lua_State *L)
{
    size_t s1len      = 0;
    unsigned char *s1 = (unsigned char *)lauxh_checklstring(L, 1, &s1len);
    size_t s2len      = 0;
    unsigned char *s2 = (unsigned char *)lauxh_checklstring(L, 2, &s2len);
    size_t len        = 0;
    unsigned char buf[BUFSIZ] = {};
    luaL_Buffer b;

    // check length
    if (s1len != s2len) {
        lua_pushfstring(
            L, "invalid arguments: both strings must be the same length");
        return lua_error(L);
    }

    // init buffer
    lua_settop(L, 2);
    luaL_buffinit(L, &b);

    // xor
    for (size_t i = 0; i < s1len; ++i) {
        buf[len++] = s1[i] ^ s2[i];
        if (len == BUFSIZ) {
            luaL_addlstring(&b, (char *)buf, len);
            len = 0;
        }
    }
    if (len) {
        luaL_addlstring(&b, (char *)buf, len);
    }

    luaL_pushresult(&b);
    return 1;
}

LUALIB_API int luaopen_postgres_strxor(lua_State *L)
{
    lua_pushcfunction(L, strxor_lua);
    return 1;
}
