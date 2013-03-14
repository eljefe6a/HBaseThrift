#!/usr/bin/python

from common import *
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase

# Connect to HBase Thrift server
transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)

# Create and open the client connection
client = Hbase.Client(protocol)
transport.open()

rows = client.getRow(tablename, "shakespeare-comedies-000001")

# Do a pull on a single row
for row in rows:
	# Pull out values in cell
	message = row.columns.get(messagecolumncf).value
	username = row.columns.get(usernamecolumncf).value
	linenumber = decode(row.columns.get(linenumbercolumncf).value)
	
	rowKey = row.row
	
	print("Got row: " + rowKey + ":" + str(linenumber) + ":" + username + ":" + message);

# Open a scan over all comedy rows in Shakespeare
scan = Hbase.TScan(startRow="shakespeare-comedies-000001", stopRow="shakespeare-comedies-999999")
scannerId = client.scannerOpenWithScan(tablename, scan)

# Go through every row passed back by scanner
row = client.scannerGet(scannerId)

while row:
	row = row[0]
	
	# Pull out values in columns
	message = row.columns.get(messagecolumncf).value
	username = row.columns.get(usernamecolumncf).value
	linenumber = decode(row.columns.get(linenumbercolumncf).value)
	
	rowKey = row.row
	
	if linenumber % 10 == 0 and message.find("again") != -1:
		print(rowKey + ":" + str(linenumber) + ":" + username + ":" + message);
	
	# Get the next row result
	row = client.scannerGet(scannerId)

# Close scanner now that we're done with it
client.scannerClose(scannerId)

# Close the client connection
transport.close()
