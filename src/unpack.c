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
#include <ctype.h>
#include <inttypes.h>
#include <string.h>
// lua
#include "lauxlib.h"

/**
 * Unpack data string with format string.
 * The format string is composed of type specifiers and length modifiers.
 *
 * Type specifiers:
 *
 *  'h' - Int16
 *  'i' - Int32
 *  's' - Null-terminated string
 *  'b' - Byte
 *  'L' - remaining number of bytes as Int32 (including its own length)
 *        this specifier must be specified only once.
 *        if this specifier is specified and the remaining number of bytes
 *        (excluding its own length) is not enough, returns nil, nil, true.
 *        if this specifier is specified and value is not greater than or equal
 *        to its own length, returns nil, error message.
 *
 * Length modifiers:
 *
 *  digit+  - length modifier that must be greater than zero.
 *            (can only be specified for the type specifier 'b')
 *  '*'     - use preceding integer value as length modifier.
 *            (can only be specified for the type specifier 'h', 'i' and 'b')
 *            if preceding integer value is negative, it is used as zero length.
 *
 * @param L Lua state
 * @return unpacked values, error message, not enough flag, consumed bytes
 * @throw string error message
 */
static int unpack_lua(lua_State *L)
{
    const char *fmt  = NULL;
    const char *data = NULL;
    const char *head = NULL;
    size_t len       = 0;
    int32_t msglen   = -1;
    intmax_t mod     = 0;
    intmax_t pre_iv  = INTMAX_MIN;
    int idx          = 1;
    union {
        int16_t i16;
        int32_t i32;
        const char str;
    } v;

    // check arguments
    luaL_checktype(L, 1, LUA_TTABLE);
    fmt  = luaL_checkstring(L, 2);
    data = luaL_checklstring(L, 3, &len);
    head = data;

    lua_settop(L, 3);
    while (*fmt != '\0') {
        const char t   = *fmt;
        intmax_t k     = 1;
        size_t consume = 0;
        void *label    = NULL;

        // check type specifier: 'h' | 'L' | 'i' | 's' | 'b'
        switch (t) {
        case 'h': // Int16
            consume = sizeof(int16_t);
            label   = &&UNPACK_NTOHS;
            break;

        case 'L': // message length as Int32
            if (msglen != -1) {
                // message length is already unpacked
                return luaL_argerror(
                    L, 1,
                    "invalid format string: message length "
                    "specifier 'L' must be specified only once");
            }
            // fall through

        case 'i': // Int32
            consume = sizeof(int32_t);
            label   = &&UNPACK_NTOHL;
            break;

        case 's': // String
            consume = sizeof(char);
            label   = &&UNPACK_STRING;
            break;

        case 'b': // Byte
            consume = sizeof(char);
            label   = &&UNPACK_BYTE;
            break;

        default: {
            char msg[128];
            snprintf(msg, sizeof(msg),
                     "invalid format string: unknown type specifier '%c'", t);
            return luaL_argerror(L, 1, msg);
        }
        }
        fmt++;

        // check length modifier: * | digit+
        if (isdigit(*fmt)) {
            if (t == 'L') {
                return luaL_argerror(
                    L, 1,
                    "invalid format string: digit length modifier can not be "
                    "specified for the type specifier 'L'");
            }

            // convert digit to integer
            mod = 0;
            do {
                mod = mod * 10 + (*fmt - '0');
                if (mod > INT32_MAX) {
                    return luaL_argerror(
                        L, 1,
                        "invalid format string: length modifier "
                        "must be less than or equal to INT32_MAX");
                }
                fmt++;
            } while (isdigit(*fmt));

            if (mod == 0) {
                return luaL_argerror(L, 1,
                                     "invalid format string: length modifier "
                                     "must be greater than zero");
            }
            k = mod;
        } else if (*fmt == '*') {
            if (t != 'h' && t != 'i' && t != 'b') {
                return luaL_argerror(
                    L, 1,
                    "invalid format string: length modifier '*' must be "
                    "specified only for the type specifier 'h', 'i' or 'b'");
            } else if (pre_iv == INTMAX_MIN) {
                return luaL_argerror(
                    L, 1,
                    "invalid format string: type specifiers with the length "
                    "modifier '*' must be preceded by the integer type "
                    "specifier 'i', 'h' or 'L' without the length modifier.");
            } else if (pre_iv < 0) {
                // negative preceding integer value to be used as zero length
                k = 0;
            } else {
                // use preceding integer value as length modifier
                k = pre_iv;
            }
            fmt++;
        } else if (t == 'b') {
            // type specifier 'b' must be followed by length modifier
            return luaL_argerror(
                L, 1,
                "invalid format string: type specifier 'b' must be followed "
                "by length modifier");
        } else {
            k = 1;
        }
        // reset preceding integer value
        pre_iv = INTMAX_MIN;

        // check the number of remaining bytes required
        consume = consume * k;
        if (len < consume) {
            // not enough
            lua_settop(L, 0);
            lua_pushnil(L);
            lua_pushnil(L);
            lua_pushboolean(L, 1);
            return 3;
        }

        // unpack data with labeled statement
        goto *label;

UNPACK_NTOHS:
        for (int i = 0; i < k; i++) {
            v.i16 = ntohs(*(int16_t *)head);
            lua_pushinteger(L, v.i16);
            lua_rawseti(L, 1, idx++);
            head += sizeof(int16_t);
        }
        if (k == 1) {
            pre_iv = v.i16;
        }
        len -= consume;
        continue;

UNPACK_NTOHL:
        for (int i = 0; i < k; i++) {
            v.i32 = ntohl(*(int32_t *)head);
            lua_pushinteger(L, v.i32);
            lua_rawseti(L, 1, idx++);
            head += sizeof(int32_t);
        }
        if (t == 'L') {
            msglen = v.i32;
            // check if remaining bytes are enough
            if (msglen < 4) {
                lua_settop(L, 0);
                lua_pushnil(L);
                lua_pushstring(
                    L, "invalid message length: message length "
                       "must be greater than or equal to its own length");
                return 2;
            } else if (len < (size_t)msglen) {
                // not enough
                lua_settop(L, 0);
                lua_pushnil(L);
                lua_pushnil(L);
                lua_pushboolean(L, 1);
                return 3;
            }
            len = msglen;
            if (*fmt == 'b') {
                // use message length as length modifier if next type specifier
                // is 'b'
                pre_iv = msglen - sizeof(int32_t);
            }
        } else if (k == 1) {
            pre_iv = v.i32;
        }
        len -= consume;
        continue;

UNPACK_STRING:
        // find null-terminated string
        for (int i = 0; i < k; i++) {
            char *tail  = memchr(head, '\0', len);
            size_t slen = 0;

            if (tail == NULL) {
                if (msglen == -1) {
                    // not enough
                    lua_settop(L, 0);
                    lua_pushnil(L);
                    lua_pushnil(L);
                    lua_pushboolean(L, 1);
                    return 3;
                }
                // message length is specified but actual length is not enough
                lua_settop(L, 0);
                lua_pushnil(L);
                lua_pushfstring(
                    L,
                    "unable to unpack string data: message length specified as "
                    "%d is insufficient to unpack the string data",
                    msglen);
                return 2;
            }
            slen = tail - head;
            lua_pushlstring(L, head, slen);
            lua_rawseti(L, 1, idx++);
            head += slen + 1;
            len -= slen + 1;
        }
        continue;

UNPACK_BYTE:
        // unpack k bytes of string
        lua_pushlstring(L, head, k);
        lua_rawseti(L, 1, idx++);
        head += k;
        len -= k;
    }

    // return number of consumed bytes
    lua_pushinteger(L, head - data);
    return 1;
}

LUALIB_API int luaopen_postgres_unpack(lua_State *L)
{
    lua_pushcfunction(L, unpack_lua);
    return 1;
}
