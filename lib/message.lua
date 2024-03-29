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
local sub = string.sub
local errorf = require('error').format
local setmetatable = setmetatable

--- @class postgres.message
--- @field consumed integer?
--- @field conn postgres.connection?
--- @field type string
local Message = {}

--- close
--- @return boolean ok
--- @return any err
--- @return boolean? timeout
function Message:close()
    return self.conn:wait_ready()
end

--- get_rows
--- @return postgres.rows? rows
function Message:get_rows()
    return nil
end

require('metamodule').new(Message)

local DECODER = {
    R = require('postgres.message.authentication').decode,
    K = require('postgres.message.backend_key_data').decode,
    ['2'] = require('postgres.message.bind_complete').decode,
    ['3'] = require('postgres.message.close_complete').decode,
    C = require('postgres.message.command_complete').decode,
    D = require('postgres.message.data_row').decode,
    I = require('postgres.message.empty_query_response').decode,
    E = require('postgres.message.error_response').decode,
    N = require('postgres.message.error_response'), -- NoticeResponse.decodee
    v = require('postgres.message.negotiation_protocol_version').decode,
    A = require('postgres.message.notification_response').decode,
    n = require('postgres.message.no_data').decode,
    t = require('postgres.message.parameter_description').decode,
    S = require('postgres.message.parameter_status').decode,
    ['1'] = require('postgres.message.parse_complete').decode,
    s = require('postgres.message.portal_suspended').decode,
    Z = require('postgres.message.ready_for_query').decode,
    T = require('postgres.message.row_description').decode,
}

--- decode_message
--- @param s string
--- @return table? msg
--- @return any err
--- @return boolean? again
local function decode(s)
    if #s < 1 then
        return nil, nil, true
    end

    local decoder = DECODER[sub(s, 1, 1)]
    if not decoder then
        return nil, errorf('unknown message type')
    end
    return decoder(s)
end

return {
    encode = {
        authentication = require('postgres.message.authentication').encode,
        backend_key_data = require('postgres.message.backend_key_data').encode,
        bind_complete = require('postgres.message.bind_complete').encode,
        bind = require('postgres.message.bind').encode,
        cancel_request = require('postgres.message.cancel_request').encode,
        close_complete = require('postgres.message.close_complete').encode,
        close = require('postgres.message.close').encode,
        describe = require('postgres.message.describe').encode,
        execute = require('postgres.message.execute').encode,
        flush = require('postgres.message.flush').encode,
        parse = require('postgres.message.parse').encode,
        password_message = require('postgres.message.password_message').encode,
        query = require('postgres.message.query').encode,
        sasl_initial_response = require('postgres.message.sasl_initial_response').encode,
        sasl_response = require('postgres.message.sasl_response').encode,
        startup_message = require('postgres.message.startup_message').encode,
        sync = require('postgres.message.sync').encode,
        terminate = require('postgres.message.terminate').encode,
    },
    decode = setmetatable({
        authentication = require('postgres.message.authentication').decode,
        backend_key_data = require('postgres.message.backend_key_data').decode,
        bind_complete = require('postgres.message.bind_complete').decode,
        bind = require('postgres.message.bind').decode,
        cancel_request = require('postgres.message.cancel_request').decode,
        close_complete = require('postgres.message.close_complete').decode,
        close = require('postgres.message.close').decode,
        command_complete = require('postgres.message.command_complete').decode,
        data_row = require('postgres.message.data_row').decode,
        describe = require('postgres.message.describe').decode,
        empty_query_response = require('postgres.message.empty_query_response').decode,
        error_response = require('postgres.message.error_response').decode,
        negotiation_protocol_version = require(
            'postgres.message.negotiation_protocol_version').decode,
        notification_response = require('postgres.message.notification_response').decode,
        no_data = require('postgres.message.no_data').decode,
        parameter_description = require('postgres.message.parameter_description').decode,
        parameter_status = require('postgres.message.parameter_status').decode,
        parse_complete = require('postgres.message.parse_complete').decode,
        portal_suspended = require('postgres.message.portal_suspended').decode,
        ready_for_query = require('postgres.message.ready_for_query').decode,
        row_description = require('postgres.message.row_description').decode,
    }, {
        __call = function(_, ...)
            return decode(...)
        end,
    }),
}
