Katta: Lucene in the cloud. Or MapFiles. Or your own custom server.

For the latest information about Katta, please visit our website at:

   http://katta.sourceforge.net/


To build katta:

	$ git clone git://katta.git.sourceforge.net/gitroot/katta/katta (read-only) 	#Check out project from git
	$ ant test
	

To start/stop a katta cluster:
	
	$ bin/start-all.sh
	$ bin/stop-all.sh


To interact with a katta cluster:
	
	$ bin/katta
	
	
To run the web-ui (port 8080):

	$ bin/katta startGui