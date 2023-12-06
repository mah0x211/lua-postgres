require('luacov')
local testcase = require('testcase')
local assert = require('assert')
local md5pswd = require('postgres.md5pswd')

function testcase.md5pswd()
    -- test that calculate MD5 password
    local pswd = md5pswd('pass', 'user', 'salt')
    assert.equal(pswd, '4950342e01923371fb6d34e764da880c')
end
