-- Encodes a Lua table as BSON.
--
-- The table can be passed as the first argument (ex. luajit encode.lua "{hello = 'world!'}")
-- or to stdin.

local bson = require "bson"

local code = ...
if not code then
	code = io.stdin:read("*a")
end

local tbl = assert(loadstring("return "..code))()

io.write(bson.Encode(tbl))
