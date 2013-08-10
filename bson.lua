
--[[
BSON codec for LuaJIT (http://bsonspec.org/#/)

The following BSON values are supported:

- Writing Lua Type -> BSON Type                   -> Reading Lua Type
---------------------------------------------------------------------
- number           -> double*                     -> number
- (not supported)  -> 32-bit integer              -> number
- int64            -> 64-bit integer*             -> int64
- boolean          -> boolean                     -> boolean
- (not supported)  -> nil                         -> nil
- string           -> string (UTF8 not supported) -> string
- table            -> subdocument                 -> table
- (not supported)  -> array                       -> table
- Bson.BinaryType  -> binary                      -> Bson.BinaryType (subtype is set to generic when writing and ignored when reading)
---------------------------------------------------------------------
Coming soon
- int8/16/32       -> 32-bit integer
- uint8/16         -> 32-bit integer
- double           -> double

* On big-endian systems, LuaJIT 2.1+ is required to use the double and 64-bit int type.
This is because BSON is stored little-endian, but the bit.bswap conversion functions only work on 32-bit
integers prior to 2.1.

All other types are not supported and will throw an error when encountered. Recursive/circular
references are also detected and will throw an error.

This library can encode tables with string keys or integer keys, which are converted to strings.
If an encoded table has both a number and a string representation of that number as keys (ex.
1 and '1'), which value is stored is undefined. Other key types will throw an error.

When decoding, if the key is a string representation of an integer ('123'), it is converted into
an integer in the resulting Lua table.

* BSON.Encode(tbl) -> string: Encodes a table
* BSON.Decode(str) -> table: Decodes a string
* BSON.BinaryType: C type for specifying data to be encoded using the binary BSON type.
  Delcaration is 'struct { int32_t size; uint8_t data[?]; }'
]]

local Bson = {}

-- Localize some values
local ffi = require "ffi"
local bit = require "bit"

local sizeof     = ffi.sizeof
local istype     = ffi.istype
local ffi_copy   = ffi.copy
local ffi_string = ffi.string

local byte       = ffi.typeof("uint8_t")
local int32      = ffi.typeof("int32_t")
local int64      = ffi.typeof("int64_t")
local double     = ffi.typeof("double")
local int32slot  = ffi.typeof("int32_t[1]")
local doubleslot = ffi.typeof("double[1]")
local int64slot  = ffi.typeof("int64_t[1]")
local int64ptr   = ffi.typeof("int64_t*")
local voidptr    = ffi.typeof("void*");

local constants = {
	double      = "\x01",
	string      = "\x02",
	subdocument = "\x03",
	array       = "\x04",
	binary      = "\x05",
	bool        = "\x08",
	int32       = "\x10",
	int64       = "\x12",
	
	bTrue       = "\x00",
	bFalse      = "\x01",
	null        = "\x00",
}

-- Setup endian conversion functions
local convertEndian, convertEndianDouble, convertEndianLong
if ffi.abi("be") then
	convertEndian = bit.bswap
	
	-- Need LuaJIT 2.1+ for bit.bswap to work on 64-bit values
	if jit.version_num >= 20100 then
		convertEndianLong = bit.bswap
		convertEndianDouble = function(v)
			local dbl = doubleslot(v)
			local int64 = ffi.cast(int64ptr, dbl)
			int64[0] = bit.bswap(int64[t])
			return dbl[0]
		end
	else
		convertEndianLong = function()
			error("Serializing 64-bit integers on big-endian systems requires LuaJIT 2.1+")
		end
		convertEndianDouble = function()
			error("Serializing doubles on big-endian systems requires LuaJIT 2.1+")
		end
	end
else
	convertEndian = function(v) return v end
	convertEndianLong = convertEndian
	convertEndianDouble = convertEndian
end

-- [De]serialization routines
local function serializeInt32(v)
	return ffi_string(int32slot(convertEndian(v)), sizeof(int32))
end

local function serializeDouble(v)
	return ffi_string(doubleslot(v), sizeof(double))
end

local function serializeInt64(v)
	return ffi_string(int64slot(convertEndianLong(v)), sizeof(int64))
end

local function deserializeInt32(str)
	local b = int32slot()
	ffi_copy(b, str, sizeof(int32))
	return convertEndian(b[0])
end

local function deserializeDouble(str)
	local b = doubleslot()
	ffi_copy(b, str, sizeof(double))
	return b[0]
end

local function deserializeInt64(str)
	local b = int64slot()
	ffi_copy(b, str, sizeof(int64))
	return b[0]
end

-- Internal encode function
local function encodeDocument(tbl, buffer, referenced)
	
	if referenced[tbl] then
		error("Recursive structure detected")
	end
	referenced[tbl] = true
	
	local size = 0
	buffer[#buffer+1] = 0
	local sizeIndex = #buffer
	
	for k,v in pairs(tbl) do
		if type(k) == "number" then
			k = tostring(k)
		elseif type(k) ~= "string" then
			error("Table keys must be strings or numbers")
		end
		
		local typ = type(v)
		
		if typ == "number" then
			buffer[#buffer+1] = constants.double
			buffer[#buffer+1] = k
			buffer[#buffer+1] = constants.null
			buffer[#buffer+1] = serializeDouble(v)
			size = size + sizeof(byte) + #k+1 + sizeof(double)
			
		elseif typ == "string" then
			buffer[#buffer+1] = constants.string
			buffer[#buffer+1] = k
			buffer[#buffer+1] = constants.null
			buffer[#buffer+1] = serializeInt32(#v+1)
			buffer[#buffer+1] = v
			buffer[#buffer+1] = constants.null
			size = size + sizeof(byte) + #k+1 + sizeof(int32) + #v+1
			
		elseif typ == "boolean" then
			buffer[#buffer+1] = constants.bool
			buffer[#buffer+1] = k
			buffer[#buffer+1] = v and constants.bTrue or constants.bFalse
			size = size + sizeof(byte)*2 + #k+1
			
		elseif typ == "table" then
			buffer[#buffer+1] = constants.subdocument
			buffer[#buffer+1] = k
			buffer[#buffer+1] = constants.null
			size = size + sizeof(byte) + #k+1 + encodeDocument(v, buffer, referenced)
			
		elseif istype(Bson.BinaryType, v) then
			buffer[#buffer+1] = constants.binary
			buffer[#buffer+1] = k
			buffer[#buffer+1] = constants.null
			buffer[#buffer+1] = serializeInt32(v.size)
			buffer[#buffer+1] = constants.null -- Subtype
			buffer[#buffer+1] = ffi_string(v.data, v.size)
			size = size + sizeof(byte) + #k+1 + sizeof(int32) + 1 + v.size
			
		elseif istype(int64, v) then
			buffer[#buffer+1] = constants.int64
			buffer[#buffer+1] = k
			buffer[#buffer+1] = constants.null
			buffer[#buffer+1] = serializeInt64(v)
			size = size + sizeof(byte) + #k+1 + sizeof(int64)
			
		else
			error("Cannot serialize value: "..tostring(v))
		end
	end
	
	buffer[#buffer+1] = constants.null
	size = size + sizeof(int32) + sizeof(byte)
	
	buffer[sizeIndex] = serializeInt32(size)
	return size
end

local function decodeDocument(str, i)
	i = i + sizeof(int32) -- skip document size; don't need it
	
	local tbl = {}
	
	while true do
		-- Read element ID
		local id = str:sub(i,i)
		i = i + 1
		
		-- If it's 0x00, end of document. Return.
		if id == constants.null then
			return tbl, i
		end
		
		-- Read key
		local k
		do
			local nexti = str:find(constants.null, i, true)
			assert(nexti, "Malformed BSON: Couldn't find key null terminator")
			k = str:sub(i, nexti-1)
			i = nexti + 1
		end
		
		-- Convert key to integer, if possible
		do
			local ki = tonumber(k)
			if ki and math.floor(ki) == ki then k = ki end
		end
		
		-- Read value
		if id == constants.double then
			tbl[k] = deserializeDouble(str:sub(i, i-1+sizeof(double)))
			i = i + sizeof(double)
			
		elseif id == constants.string then
			local size = deserializeInt32(str:sub(i, i-1+sizeof(int32)))
			assert(size >= 1, "Malformed BSON: Invalid string size")
			i = i + sizeof(int32)
			
			tbl[k] = str:sub(i, i+size-2)
			i = i + size
			
		elseif id == constants.subdocument or id == constants.array then
			tbl[k], i = decodeDocument(str, i)
		
		elseif id == constants.binary then
			local size = deserializeInt32(str:sub(i, i-1+sizeof(int32)))
			assert(size >= 1, "Malformed BSON: Invalid binary size")
			i = i + sizeof(int32) + 1 -- Skip the subtype byte
			
			local bin = Bson.BinaryType(size)
			bin.size = size
			ffi_copy(bin.data, str:sub(i, i+size-1), size)
			tbl[k] = bin
			i = i + size
			
		elseif id == constants.bool then
			tbl[k] = str:sub(i,i) ~= constants.bFalse
			i = i + 1
			
		elseif id == constants.int32 then
			tbl[k] = deserializeInt32(str:sub(i, i-1+sizeof(int32)))
			i = i + sizeof(int32)
			
		elseif id == constants.int64 then
			tbl[k] = deserializeInt64(str:sub(i, i-1+sizeof(int64)))
			i = i + sizeof(int64)
			
		else
			error("Unknown/unsupported BSON type: 0x"..bit.tohex(string.byte(id)))
		end
	end
end

-- ------------------------------------------------------------------

-- Type for binary data.
Bson.BinaryType = ffi.typeof("struct { int32_t size; uint8_t data[?]; }")

-- Converts a table to a BSON-encoded Lua string.
function Bson.Encode(tbl)
	assert(type(tbl) == "table", "Bson.Encode takes a table")
	
	local buffer = {}
	encodeDocument(tbl, buffer, {})
	return table.concat(buffer, "")
end

-- Reads a table from a BSON-encoded Lua string
function Bson.Decode(str)
	return (decodeDocument(str, 1)) -- Adjust to one result: the table.
end

return Bson
