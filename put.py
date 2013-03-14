#!/usr/bin/python

from common import *
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase
import os
import os.path

# Connect to HBase Thrift server
transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)

# Create and open the client connection
client = Hbase.Client(protocol)
transport.open()

# Disable and delete table if table exists already
tables = client.getTableNames()

found = False

for table in tables:
	if table.name == tablename:
		found = True

if found == True:
	client.disableTable(tablename)
	client.deleteTable(tablename)

# Create the table
client.createTable(tablename, [Hbase.ColumnDescriptor(name=cfname)])

# Create a message for every line in every work of Shakespeare
sourceDir = "shakespeare"

for filename in os.listdir(sourceDir):
	shakespeare = open(os.path.join(sourceDir, filename), "rb")
	
	linenumber = 0;
	
	# Create a list of mutations per work of Shakespeare
	mutationsbatch = []
	
	for line in shakespeare:
		rowkey = username + "-" + filename + "-" + str(linenumber).zfill(6)
		
		# Create an array containing all values for the Column Descriptors
		mutations = [
			Hbase.Mutation(column=messagecolumncf, value=line.strip()),
			Hbase.Mutation(column=linenumbercolumncf, value=encode(linenumber)),
			Hbase.Mutation(column=usernamecolumncf, value=username)
		]
		
		# Add the new mutations to the MutationsBatch list
		mutationsbatch.append(Hbase.BatchMutation(row=rowkey,mutations=mutations))
		
		linenumber = linenumber + 1
	
	# Run the mutations for the work of Shakespeare
	client.mutateRows(tablename, mutationsbatch)
	
client.close()
