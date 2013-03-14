#!/usr/bin/env python
import struct
import base64

# The host and port to connect to Thrift server
host = "localhost"
port = 9090
numRows = 1000

tablename = "messagestable"
cfname = "messages"

# Column Descriptor names
messagecolumn = "message"
usernamecolumn = "username"
linenumbercolumn = "line"

# Column Family and Column Descriptor names separated by a colon
# This is how HBase expects the names to be formatted
messagecolumncf = cfname + ":" + messagecolumn
usernamecolumncf = cfname + ":" + usernamecolumn
linenumbercolumncf = cfname + ":" + linenumbercolumn

username = "shakespeare"

# Method for encoding ints with Thrift's string encoding
def encode(n):
	return struct.pack("i", n)

# Method for decoding ints with Thrift's string encoding
def decode(s):
	return struct.unpack('i', s)[0]