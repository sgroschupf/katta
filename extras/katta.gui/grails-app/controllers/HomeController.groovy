import java.util.ArrayList;

import org.I0Itec.zkclient.ZkClient;

import java.util.List;


public class HomeController {
	// zkService
	def zkService
	
	def index = {
			List clustersFromDB =  Cluster.findAll()
			List clusters = new ArrayList()
			for (Cluster cluster : clustersFromDB) {
				ZkClient zkClient = zkService.getZkConnection(cluster.name, cluster.zkUrl); 
				int nodeCount = zkClient.getChildren("/katta/nodes").size
				int indexCount =  zkClient.getChildren("/katta/indexes").size
				clusters.add([id:cluster.id, name:cluster.name, uri:cluster.zkUrl, nodeCount:nodeCount, indexCount:indexCount])
			}

			List fileSystems = LinkedFs.findAll()
			[clusters:clusters, fileSystems:fileSystems]
	}
}