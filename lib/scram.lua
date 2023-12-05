--
-- Copyright (C) 2022 Masatoshi Fukunaga
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
local find = string.find
local match = string.match
local sub = string.sub
local concat = table.concat
local errorf = require('error').format
local random = require('string.random')
local hmac_sha256 = require('hmac').sha256
local encode_base64 = require('base64mix').encode
local decode_base64 = require('base64mix').decode
local strxor = require('postgres.strxor')

--- do_hmac_sha256 - calculate HMAC-SHA256
--- @param key string?
--- @param data string
--- @return string
local function do_hmac_sha256(key, data)
    local ctx = hmac_sha256(key)
    ctx:update(data)
    return ctx:final(true)
end

--- do_sha256 - calculate SHA256
--- @param data string
--- @return string
local function do_sha256(data)
    return do_hmac_sha256(nil, data)
end

--- pbkdf2 - Password-Based Key Derivation Function 2
--- @param password string
--- @param salt string
--- @param niter number
--- @return string
local function pbkdf2(password, salt, niter)
    -- calculate first block
    local block = do_hmac_sha256(password, salt .. '\0\0\0\1')
    local res = block
    -- iterate remaining times
    for _ = 2, niter do
        block = do_hmac_sha256(password, block)
        res = strxor(res, block)
    end
    return res
end

--- decode_kvpair
--- @param data string
--- @return table<string, string>? kvpair
--- @return any err
--- @return string? trailing_data
local function decode_kvpair(data)
    local s = data .. ','
    local kvpair = {}
    local head = 1
    local tail = find(s, ',', head, true)
    while tail do
        local kv = sub(s, head, tail - 1)
        if #kv > 0 then
            local k, v = match(kv, '^([^=]+)=(.*)$')
            if not k or not v then
                return nil, errorf('%q is non key-value pair format', kv)
            end
            kvpair[k] = v
        end
        head = tail + 1
        tail = find(s, ',', head, true)
    end

    return kvpair, nil, head < #s and sub(s, head) or nil
end

--- @class postgres.scram
--- @field private username string
--- @field private password string
--- @field private nonce string
--- @field private salted_password? string
--- @field private auth_message? string
local SCRAM = {}

--- init
--- @param username string
--- @param password string
function SCRAM:init(username, password)
    assert(type(username) == 'string', 'username must be string')
    assert(type(password) == 'string', 'password must be string')

    self.username = username
    self.password = password
    self.nonce = encode_base64(random(18))
    return self
end

--- client_first_message
--- @return string
function SCRAM:client_first_message()
    return 'n,,n=' .. self.username .. ',r=' .. self.nonce
end

--- client_final_message
--- @param server_first_message string
--- @return string? client_final_message
--- @return any err
function SCRAM:client_final_message(server_first_message)
    assert(type(server_first_message) == 'string',
           'server_first_message must be string')

    -- extract server_first_message
    -- "r=<nonce>,s=<salt>,i=<iteration count>",
    local s = server_first_message .. ','
    local kvp, err = decode_kvpair(s)
    if not kvp then
        return nil, errorf('invalid server_first_message %q: %s', err)
    end

    local nonce, salt, niter = kvp.r, kvp.s, tonumber(kvp.i)
    if not nonce then
        return nil,
               errorf('invalid server_first_message %q: nonce is not specified',
                      server_first_message)
    elseif not salt then
        return nil,
               errorf('invalid server_first_message %q: salt is not specified',
                      server_first_message)
    elseif not niter then
        return nil,
               errorf(
                   'invalid server_first_message %q: iteration count is not specified',
                   server_first_message)
    end

    niter = tonumber(niter)
    if niter < 4096 then
        return nil, errorf(
                   'invalid server_first_message %q: iteration count is not greater than or equal to 4096',
                   server_first_message)
    end

    self.salted_password = pbkdf2(self.password, decode_base64(salt), niter)
    self.auth_message = concat({
        'n=',
        self.username,
        ',r=',
        self.nonce,
        ',',
        server_first_message,
        ',c=biws,r=',
        nonce,
    })
    local client_key = do_hmac_sha256(self.salted_password, 'Client Key')
    local stored_key = do_sha256(client_key)
    local client_signature = do_hmac_sha256(stored_key, self.auth_message)
    local client_proof = encode_base64(strxor(client_key, client_signature))

    -- client_final_message without channel binding
    return 'c=biws,r=' .. nonce .. ',p=' .. client_proof
end

--- verify_server_signature
--- @param server_final_message string
--- @return boolean
function SCRAM:verify_server_signature(server_final_message)
    assert(type(server_final_message) == 'string',
           'server_final_message must be string')

    -- extract server_final_message
    -- "v=<server-signature>"
    local kvp, err = decode_kvpair(server_final_message)
    if not kvp then
        return nil, errorf('invalid server_final_message %q: %s', err)
    end

    local server_signature = kvp.v
    if not server_signature then
        return nil,
               errorf(
                   'invalid server_final_message %q: server-signature is not specified',
                   server_final_message)
    end

    local server_key = do_hmac_sha256(self.salted_password, 'Server Key')
    local exptected = encode_base64(
                          do_hmac_sha256(server_key, self.auth_message))
    if server_signature == exptected then
        return true
    end
    return false,
           errorf(
               'invalid server_final_message %q: server-signature %q (expected %q)',
               server_final_message, server_signature, exptected)
end

return {
    new = require('metamodule').new(SCRAM),
}
