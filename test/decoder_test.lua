require('luacov')
local testcase = require('testcase')
local assert = require('assert')
local new_decoder = require('postgres.decoder').new
local new_connection = require('postgres.connection').new

function testcase.new()
    -- test that create a new postgres.decoder
    local decoder = assert(new_decoder())
    assert.match(decoder, '^postgres%.decoder: ', false)
end

function testcase.register()
    local decoder = assert(new_decoder())

    -- test that registers a decode function for specified oid and type name
    decoder:register(25, 'text', function(val)
        return val .. '!!!'
    end)
    local v, err = decoder:decode_by_oid(25, 'hello')
    assert.is_nil(err)
    assert.equal(v, 'hello!!!')
end

function testcase.no_decode()
    local decoder = assert(new_decoder())

    -- test that registers a decode function for specified oid and type name
    local v, err = decoder:decode_by_oid(-1234567, 'hello')
    assert.is_nil(err)
    assert.equal(v, 'hello')
end

function testcase.decode_boolean_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode boolean array
    local res = assert(c:query([[
        SELECT ARRAY[true, false, true] AS boolean_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        true,
        false,
        true,
    })
end

function testcase.decode_int_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode int array
    local res = assert(c:query([[
        SELECT ARRAY[1,2,3] AS int_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        1,
        2,
        3,
    })
end

function testcase.decode_int_mrange()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode int multi-range
    local res = assert(c:query([[
        SELECT '{[1,3),[5,7)}'::int4multirange AS int_mrange;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        [1] = {
            [1] = 1,
            [2] = 3,
            lower_inc = true,
        },
        [2] = {
            [1] = 5,
            [2] = 7,
            lower_inc = true,
        },
    })
end

function testcase.decode_int_range_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode int range array
    local res = assert(c:query([[
        SELECT ARRAY['[1,3)'::int4range] AS int_range_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        [1] = {
            [1] = 1,
            [2] = 3,
            lower_inc = true,
        },
    })
end

function testcase.decode_int_mrange_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode int multi-range array
    local res = assert(c:query([[
        SELECT ARRAY['{[1,3),[5,7)}'::int4multirange] AS int_mrange_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            [1] = {
                [1] = 1,
                [2] = 3,
                lower_inc = true,
            },
            [2] = {
                [1] = 5,
                [2] = 7,
                lower_inc = true,
            },
        },
    })
end

function testcase.decode_float_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode int array
    local res = assert(c:query([[
        SELECT ARRAY[1.2,2.3,3.4] AS float_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        1.2,
        2.3,
        3.4,
    })
end

function testcase.decode_float_range()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode int multi-range
    local res = assert(c:query([[
        SELECT '[1.2,3.4)'::numrange AS float_range;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        [1] = 1.2,
        [2] = 3.4,
        lower_inc = true,
    })
end

function testcase.decode_float_mrange()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode int multi-range
    local res = assert(c:query([[
        SELECT '{[1.2,3.4),[5.6,7.8)}'::nummultirange AS float_mrange;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        [1] = {
            [1] = 1.2,
            [2] = 3.4,
            lower_inc = true,
        },
        [2] = {
            [1] = 5.6,
            [2] = 7.8,
            lower_inc = true,
        },
    })
end

function testcase.decode_float_range_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode int multi-range
    local res = assert(c:query([[
        SELECT ARRAY[
            '[1.2,3.4)'::numrange
        ] AS float_range_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            [1] = 1.2,
            [2] = 3.4,
            lower_inc = true,
        },
    })
end

function testcase.decode_float_mrange_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode int multi-range
    local res = assert(c:query([[
        SELECT ARRAY[
            '{[1.2,3.4),[5.6,7.8)}'::nummultirange
        ] AS float_mrange_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            [1] = {
                [1] = 1.2,
                [2] = 3.4,
                lower_inc = true,
            },
            [2] = {
                [1] = 5.6,
                [2] = 7.8,
                lower_inc = true,
            },
        },
    })
end

function testcase.decode_date_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode date array
    local res = assert(c:query([[
        SELECT ARRAY[CURRENT_DATE, CURRENT_DATE, CURRENT_DATE] AS date_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(#v, 3)
    for _, date in ipairs(v) do
        assert.is_int(date.year)
        assert.is_int(date.month)
        assert.is_int(date.day)
    end
end

function testcase.decode_date_range()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode date range
    local res = assert(c:query([[
        SELECT '[1999-05-12, 1999-12-25)'::daterange AS date_range;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            year = 1999,
            month = 5,
            day = 12,
        },
        {
            year = 1999,
            month = 12,
            day = 25,
        },
        lower_inc = true,
    })
end

function testcase.decode_date_mrange()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode date multi-range
    local res = assert(c:query([[
        SELECT datemultirange(
            '[1999-05-12, 1999-7-25)'::daterange,
            '[1999-12-01, 1999-12-31)'::daterange
        ) AS date_multirange;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            {
                year = 1999,
                month = 5,
                day = 12,
            },
            {
                year = 1999,
                month = 7,
                day = 25,
            },
            lower_inc = true,
        },
        {
            {
                year = 1999,
                month = 12,
                day = 1,
            },
            {
                year = 1999,
                month = 12,
                day = 31,
            },
            lower_inc = true,
        },
    })
end

function testcase.decode_date_range_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode date range array
    local res = assert(c:query([[
        SELECT ARRAY['[1999-05-12, 1999-12-25)'::daterange] AS date_range_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            {
                year = 1999,
                month = 5,
                day = 12,
            },
            {
                year = 1999,
                month = 12,
                day = 25,
            },
            lower_inc = true,
        },
    })
end

function testcase.decode_date_mrange_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode date multi-range array
    local res = assert(c:query([[
        SELECT ARRAY[datemultirange(
            '[1999-05-12, 1999-7-25)'::daterange,
            '[1999-12-01, 1999-12-31)'::daterange
        )] AS date_multirange_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            {
                {
                    year = 1999,
                    month = 5,
                    day = 12,
                },
                {
                    year = 1999,
                    month = 7,
                    day = 25,
                },
                lower_inc = true,
            },
            {
                {
                    year = 1999,
                    month = 12,
                    day = 1,
                },
                {
                    year = 1999,
                    month = 12,
                    day = 31,
                },
                lower_inc = true,
            },
        },
    })
end

function testcase.decode_time_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode time array
    local res = assert(c:query([[
        SELECT ARRAY[
            '11:59:12'::time,
            '11:59:12'::time,
            '11:59:12'::time
        ] AS time_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            hour = 11,
            min = 59,
            sec = 12,
            usec = 0,
        },
        {
            hour = 11,
            min = 59,
            sec = 12,
            usec = 0,
        },
        {
            hour = 11,
            min = 59,
            sec = 12,
            usec = 0,
        },
    })
end

function testcase.decode_timestamp_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode timestamp array
    local res = assert(c:query([[
        SELECT ARRAY[
            '1999-12-1 13:59:59.123456+00'::timestamptz,
            '1999-12-1 13:59:59.123456+00'::timestamptz,
            '1999-12-1 13:59:59.123456+00'::timestamptz
        ] AS timestamp_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            year = 1999,
            month = 12,
            day = 1,
            hour = 13,
            min = 59,
            sec = 59,
            usec = 123456,
            tz = '+',
            tzhour = 0,
            tzmin = 0,
            tzsec = 0,
        },
        {
            year = 1999,
            month = 12,
            day = 1,
            hour = 13,
            min = 59,
            sec = 59,
            usec = 123456,
            tz = '+',
            tzhour = 0,
            tzmin = 0,
            tzsec = 0,
        },
        {
            year = 1999,
            month = 12,
            day = 1,
            hour = 13,
            min = 59,
            sec = 59,
            usec = 123456,
            tz = '+',
            tzhour = 0,
            tzmin = 0,
            tzsec = 0,
        },
    })
end

function testcase.decode_tsrange()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode tsrange
    local res = assert(c:query([[
        SELECT '[1999-12-1 13:59:59.123456+00, 1999-12-1 20:00:00.123456+00)'::tsrange
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        [1] = {
            year = 1999,
            month = 12,
            day = 1,
            hour = 13,
            min = 59,
            sec = 59,
            usec = 123456,
        },
        [2] = {
            year = 1999,
            month = 12,
            day = 1,
            hour = 20,
            min = 0,
            sec = 0,
            usec = 123456,
        },
        lower_inc = true,
    })
end

function testcase.decode_tsmrange()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode tsmultirange
    local res = assert(c:query([[
        SELECT '{[1999-12-1 13:59:59.123456+00, 1999-12-1 20:00:00.123456+00)}'::tsmultirange
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            [1] = {
                year = 1999,
                month = 12,
                day = 1,
                hour = 13,
                min = 59,
                sec = 59,
                usec = 123456,
            },
            [2] = {
                year = 1999,
                month = 12,
                day = 1,
                hour = 20,
                min = 0,
                sec = 0,
                usec = 123456,
            },
            lower_inc = true,
        },
    })
end

function testcase.decode_tsrange_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode tsrange array
    local res = assert(c:query([[
        SELECT ARRAY[
            '[1999-12-1 13:59:59.123456+00, 1999-12-1 20:00:00.123456+00)'::tsrange
        ] AS tsrange_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            [1] = {
                year = 1999,
                month = 12,
                day = 1,
                hour = 13,
                min = 59,
                sec = 59,
                usec = 123456,
            },
            [2] = {
                year = 1999,
                month = 12,
                day = 1,
                hour = 20,
                min = 0,
                sec = 0,
                usec = 123456,
            },
            lower_inc = true,
        },
    })
end

function testcase.decode_tsmrange_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode tsmultirange array
    local res = assert(c:query([[
        SELECT ARRAY[
            '{[1999-12-1 13:59:59.123456+00, 1999-12-1 20:00:00.123456+00)}'::tsmultirange
        ] AS tsmrange_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            {
                [1] = {
                    year = 1999,
                    month = 12,
                    day = 1,
                    hour = 13,
                    min = 59,
                    sec = 59,
                    usec = 123456,
                },
                [2] = {
                    year = 1999,
                    month = 12,
                    day = 1,
                    hour = 20,
                    min = 0,
                    sec = 0,
                    usec = 123456,
                },
                lower_inc = true,
            },
        },
    })
end

function testcase.decode_bytea_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode bytea array
    local res = assert(c:query([[
        SELECT ARRAY[
            '\x1234'::bytea,
            '\x5678'::bytea
        ] AS bytea_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        '\\x1234',
        '\\x5678',
    })
end

function testcase.decode_bit_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode bit array
    local res = assert(c:query([[
        SELECT ARRAY[
            B'0110111001100001',
            B'10010101'
        ] AS bit_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            110,
            97,
        },
        {
            149,
        },
    })
end

function testcase.decode_tsvector_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode tsvector array
    local res = assert(c:query([[
        SELECT ARRAY[
            'a and ate cat fat mat on rat sat'::tsvector
        ] AS tsvector_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            {
                lexeme = 'a',
            },
            {
                lexeme = 'and',
            },
            {
                lexeme = 'ate',
            },
            {
                lexeme = 'cat',
            },
            {
                lexeme = 'fat',
            },
            {
                lexeme = 'mat',
            },
            {
                lexeme = 'on',
            },
            {
                lexeme = 'rat',
            },
            {
                lexeme = 'sat',
            },
        },
    })
end

function testcase.decode_point_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode point array
    local res = assert(c:query([[
        SELECT ARRAY[
            '(1.5, 2.5)'::point,
            '(3.5, 4.5)'::point
        ] AS point_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            1.5,
            2.5,
        },
        {
            3.5,
            4.5,
        },
    })
end

function testcase.decode_line_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode line array
    local res = assert(c:query([[
        SELECT ARRAY[
            '{1.5, 2, 2.5}'::line,
            '{3.5, 4, 4.5}'::line
        ] AS line_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            1.5,
            2.0,
            2.5,
        },
        {
            3.5,
            4.0,
            4.5,
        },
    })
end

function testcase.decode_lseg_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode lseg array
    local res = assert(c:query([[
        SELECT ARRAY[
            '[(1.5, 2.5), (3.5, 4.5)]'::lseg,
            '[(5.5, 6.5), (7.5, 8.5)]'::lseg
        ] AS lseg_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            {
                1.5,
                2.5,
            },
            {
                3.5,
                4.5,
            },
        },
        {
            {
                5.5,
                6.5,
            },
            {
                7.5,
                8.5,
            },
        },
    })
end

function testcase.decode_box_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode box array
    local res = assert(c:query([[
        SELECT ARRAY[
            '(3.5, 4.5), (1.5, 2.5)'::box,
            '(7.5, 8.5), (5.5, 6.5)'::box
        ] AS box_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            {
                3.5,
                4.5,
            },
            {
                1.5,
                2.5,
            },
        },
        {
            {
                7.5,
                8.5,
            },
            {
                5.5,
                6.5,
            },
        },
    })
end

function testcase.decode_path_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode path array
    local res = assert(c:query([[
        SELECT ARRAY[
            '((1.5, 2.5), (3.5, 4.5))'::path,
            '((5.5, 6.5), (7.5, 8.5))'::path
        ] AS path_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            {
                1.5,
                2.5,
            },
            {
                3.5,
                4.5,
            },
        },
        {
            {
                5.5,
                6.5,
            },
            {
                7.5,
                8.5,
            },
        },
    })
end

function testcase.decode_polygon_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode polygon array
    local res = assert(c:query([[
        SELECT ARRAY[
            '((1.5, 2.5), (3.5, 4.5), (5.5, 6.5))'::polygon,
            '((7.5, 8.5), (9.5, 10.5), (11.5, 12.5))'::polygon
        ] AS polygon_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            {
                1.5,
                2.5,
            },
            {
                3.5,
                4.5,
            },
            {
                5.5,
                6.5,
            },
        },
        {
            {
                7.5,
                8.5,
            },
            {
                9.5,
                10.5,
            },
            {
                11.5,
                12.5,
            },
        },
    })
end

function testcase.decode_circle_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode circle array
    local res = assert(c:query([[
        SELECT ARRAY[
            '<(1.5, 2.5), 3.5>'::circle,
            '<(4.5, 5.5), 6.5>'::circle
        ] AS circle_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            1.5,
            2.5,
            3.5,
        },
        {
            4.5,
            5.5,
            6.5,
        },
    })
end

function testcase.decode_text_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode text array
    local res = assert(c:query([[
        SELECT ARRAY[
            'foo bar'::text,
            'baz_qux'::text
        ] AS text_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        'foo bar',
        'baz_qux',
    })
end

function testcase.decode_json_array()
    local decoder = assert(new_decoder())
    local c = assert(new_connection())

    -- test that decode json array
    local res = assert(c:query([[
        SELECT ARRAY[
            '{"a": 1, "b": 2}'::json,
            '{"c": 3, "d": 4}'::json,
            '{"e": 5, "f": 6}'::json
        ] AS json_array;
    ]]))
    local rows = assert(res:get_rows())
    assert(rows:next())
    local field, val = assert(rows:read())
    local v = assert(decoder:decode_by_oid(field.type_oid, val))
    assert.equal(v, {
        {
            a = 1,
            b = 2,
        },
        {
            c = 3,
            d = 4,
        },
        {
            e = 5,
            f = 6,
        },
    })
end

