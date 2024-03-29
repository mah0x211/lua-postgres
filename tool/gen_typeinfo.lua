--
-- this script generate typeinfo.lua file
--
local SELECT_DATATYPES = [[
SELECT
    t.oid AS oid,
    pg_catalog.format_type(t.oid, NULL) AS "name",
    CASE t.typtype
        WHEN 'b' THEN 'base'
        WHEN 'c' THEN 'composite'
        WHEN 'd' THEN 'domain'
        WHEN 'e' THEN 'enum'
        WHEN 'p' THEN 'pseudo'
        WHEN 'r' THEN 'range'
        WHEN 'm' THEN 'multirange'
        ELSE 'unknown'
    END AS "type",
    t.typtype AS "type_code",
    t.typarray AS "array_oid",
    CASE t.typcategory
        WHEN 'A' THEN 'array'
        WHEN 'B' THEN 'boolean'
        WHEN 'C' THEN 'composite'
        WHEN 'D' THEN 'datetime'
        WHEN 'E' THEN 'enum'
        WHEN 'G' THEN 'geometric'
        WHEN 'I' THEN 'net'
        WHEN 'N' THEN 'numeric'
        WHEN 'P' THEN 'pseudo'
        WHEN 'R' THEN 'range'
        WHEN 'S' THEN 'string'
        WHEN 'T' THEN 'timespan'
        WHEN 'U' THEN 'user_defined'
        WHEN 'V' THEN 'bit'
        WHEN 'X' THEN 'unknown'
        WHEN 'Z' THEN 'internal'
        ELSE 'unknown'
    END AS "category",
    t.typcategory AS "category_code",
    pg_catalog.obj_description(t.oid, 'pg_type') AS "description"
FROM
    pg_catalog.pg_type t
WHERE
    pg_catalog.pg_type_is_visible(t.oid) = TRUE
AND
    t.typisdefined = TRUE
AND
    -- without pseudo-type
    t.typtype != 'p'
-- AND
--     -- non-composite type or kind of the composite type class
--     (t.typrelid = 0 OR (
--         SELECT
--             c.relkind = 'c'
--         FROM
--             pg_catalog.pg_class c
--         WHERE c.oid = t.typrelid
--     ))
-- AND
--     NOT EXISTS(
--         SELECT
--             1
--         FROM
--             pg_catalog.pg_type el
--         WHERE
--             el.oid = t.typelem
--         AND
--             el.typarray = t.oid
--     )

ORDER BY t.oid;
]]

local format = string.format
local find = string.find
local gsub = string.gsub
local concat = table.concat
local new_connection = require('postgres.connection').new

local c = assert(new_connection())
local res = assert(c:query(SELECT_DATATYPES))

local maxwidth = 0
do
    local stat = assert(res:stat())
    for _, field in ipairs(stat.fields) do
        maxwidth = math.max(maxwidth, #field.name)
    end
end

local rows = assert(res:get_rows())
local comment_fmt = '-- %' .. maxwidth .. 's: %s'
local typelist = {}
local array2base = {}
while rows:next() do
    local field, v = rows:read()
    local comments = {}
    local typeinfo = {
        comments = comments,
    }
    while field do
        if v then
            local name = field.name
            local val = gsub(v, '"', '')
            typeinfo[field.name] = val
            if name ~= 'array_oid' or v ~= '0' then
                comments[#comments + 1] = format(comment_fmt, name, val)
            end
        end
        field, v = rows:read()
    end

    -- skip system types and pseudo-types and snapshots
    local name = typeinfo.name
    if not find(name, '^pg_') and not find(name, '^reg') and
        not find(name, 'snapshot') and
        (not find(name, 'vector') or find(name, '^tsvector')) then
        typelist[#typelist + 1] = typeinfo
        array2base[typeinfo.array_oid] = typeinfo.name
    end
end
res:close()

for _, typeinfo in ipairs(typelist) do

    -- generate decoder name
    local name = typeinfo.name
    local base = array2base[typeinfo.oid]
    if base then
        name = base .. '_array'
    elseif typeinfo.category == 'array' then
        -- skip array type without base type
        name = nil
    end

    if name then
        local decoder = format('decode_%s', gsub(name, ' ', '_'))

        local repl = {
            ['$OID'] = typeinfo.oid,
            ['$NAME'] = format('%q', typeinfo.name),
            ['$FN'] = decoder,
        }
        local code = gsub([[
OID2NAME[$OID] = $NAME
NAME2DEC[$NAME] = $FN
]], '%$%w+', repl)
        print(concat(typeinfo.comments, '\n'))
        print(code)
    end
end

