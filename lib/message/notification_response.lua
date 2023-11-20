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
local find = string.find
local sub = string.sub
local errorf = require('error').format
local ntohl = require('postgres.ntohl')
--- constants
local NULL = '\0'

--- @class postgres.message.notification_response : postgres.message
--- @field pid integer
--- @field channel string
--- @field payload string
local NotificationResponse = require('metamodule').new({}, 'postgres.message')

--- decode
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    --
    -- NotificationResponse (B)
    --   Byte1('A')
    --     Identifies the message as a notification response.
    --
    --   Int32
    --     Length of message contents in bytes, including self.
    --
    --   Int32
    --     The process ID of the notifying backend process.
    --
    --   String
    --     The name of the channel that the notify has been raised on.
    --
    --   String
    --     The "payload" string passed from the notifying process.
    --
    if #s < 5 then
        return nil, nil, true
    elseif sub(s, 1, 1) ~= 'A' then
        return nil, errorf('invalid NotificationResponse message')
    end

    local len = ntohl(sub(s, 2))
    local consumed = len + 1
    if #s < consumed then
        return nil, nil, true
    end

    local msg = NotificationResponse()
    msg.type = 'NotificationResponse'
    msg.consumed = consumed

    -- extract pid
    local head = 6
    msg.pid = ntohl(sub(s, head))
    head = head + 4

    -- extract null-terminated channel name
    local tail = find(s, NULL, head, true)
    if not tail or tail > consumed then
        return nil, errorf('invalid NotificationResponse message')
    end
    msg.channel = sub(s, head, tail - 1)
    head = tail + 1

    -- extract null-terminated payload
    tail = find(s, NULL, head, true)
    if not tail or tail > consumed then
        return nil, errorf('invalid NotificationResponse message')
    end
    msg.payload = sub(s, head, tail - 1)

    return msg
end

return {
    decode = decode,
}
