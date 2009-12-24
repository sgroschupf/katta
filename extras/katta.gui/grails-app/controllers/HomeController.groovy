import java.util.ArrayList;

import net.sf.katta.protocol.InteractionProtocol;


import java.util.List;


public class HomeController {
	// zkService
	def zkService
	
	def index = {
		List clustersFromDB =  Cluster.findAll()
		List clusters = new ArrayList()
		for (Cluster cluster : clustersFromDB) {
			InteractionProtocol protocol = zkService.getProtocol(cluster); 
			int nodeCount = protocol.getLiveNodes().size
			int indexCount =  protocol.getIndices().size
			clusters.add([id:cluster.id, name:cluster.name, uri:cluster.zkUrl, nodeCount:nodeCount, indexCount:indexCount])
		}
		
		List fileSystems = LinkedFs.findAll()
		[clusters:clusters, fileSystems:fileSystems]
	}
}