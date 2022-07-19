package = "postgres"
version = "dev-1"
source = {
    url = "git+https://github.com/mah0x211/lua-postgres.git",
}
description = {
    summary = "PostgreSQL client for lua",
    homepage = "https://github.com/mah0x211/lua-postgres",
    license = "MIT/X11",
    maintainer = "Masatoshi Fukunaga",
}
dependencies = {
    "lua >= 5.1",
    "errno >= 0.3.0",
    "gpoll >= 0.1.0",
    "io-wait >= 0.1.0",
    "isa >= 0.3.0",
    "lauxhlib >= 0.5.0",
    "libpq",
    "metamodule >= 0.3.3",
    "unpack >= 0.1.0",
    "yyjson >= 0.4.0",
}
build = {
    type = "builtin",
    modules = {
        ["postgres"] = "postgres.lua",
        ["postgres.connection"] = "lib/connection.lua",
        ["postgres.result"] = "lib/result.lua",
        ["postgres.reader"] = "lib/reader.lua",
        ["postgres.reader.single"] = "lib/reader/single.lua",
    },
}
