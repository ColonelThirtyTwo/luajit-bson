-- Decodes BSON into a Lua table.
--
-- BSON is read from stdin.

local bson = require "bson"

local function printTable(tbl, indent)
	local previndent = (indent or "")
	indent = previndent .. "\t"
	
	io.write("{\n")
	
	for k,v in pairs(tbl) do
		io.write(indent)
		
		if type(k) == "number" then
			io.write("[", tostring(k), "]")
		else
			io.write("[", string.format("%q", k), "]")
		end
		
		io.write(" = ")
		
		local typ = type(v)
		if typ == "table" then
			printTable(v, indent)
		elseif typ == "string" then
			io.write(string.format("%q", v))
		else
			io.write(tostring(v))
		end
		io.write(",\n")
	end
	
	io.write(previndent, "}")
end

printTable(bson.Decode(io.stdin:read("*a")))
io.write("\n")
