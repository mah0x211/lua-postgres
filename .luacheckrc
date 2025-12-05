std = "max"
include_files = {
    "lib/**/*.lua",
    "test/**/*_test.lua",
}
exclude_files = {
    "_*.lua",
}
ignore = {
    'assert',
    -- unused argument
    '212',
    -- line is too long
    '631',
}
