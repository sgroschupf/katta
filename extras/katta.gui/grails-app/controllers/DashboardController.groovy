import java.util.ArrayList;
import java.util.List;

import net.sf.katta.index.AssignedShard;
import net.sf.katta.index.IndexMetaData;
import net.sf.katta.master.MasterMetaData;
import net.sf.katta.node.NodeMetaData;
import org.I0Itec.zkclient.ZkClient;
import grails.converters.JSON

class DashboardController {
    
    def zkService
    
    def index = {
    		def cluster = Cluster.get(params.id)
    		ZkClient zkClient = zkService.getZkConnection(cluster.name, cluster.zkUrl); 
    		//master
    		MasterMetaData master = zkClient.readData(cluster.rootNode+"/master")
    		// nodes
    		List nodeNames = zkClient.getChildren(cluster.rootNode+"/nodes")
    		List nodes = new ArrayList()
    		for (String nodeName : nodeNames) {
    			NodeMetaData meta = zkClient.readData(cluster.rootNode+"/nodes/"+nodeName)
    			int assigned = zkClient.getChildren(cluster.rootNode+"/node-to-shard/"+meta.name).size
    			nodes.add([id:meta.name.replace(":",""), name:meta.name, state:meta.state, started:meta.startTimeAsDate, assigned:assigned])
    		}
    		// indexes
    		List indexNames = zkClient.getChildren(cluster.rootNode+"/indexes")
    		List indexes = new ArrayList()
    		for (String indexName : indexNames) {
    			IndexMetaData meta = zkClient.readData(cluster.rootNode+"/indexes/"+indexName);
    			indexes.add([name:meta.name, path:meta.path, state:meta.state,repLevel:meta.replicationLevel]);
    		}
    		
    		
    		[master:master, nodes:nodes, indexes:indexes, charts:grailsApplication.config.katta.dashboard.charts]
    	}

    	def getValues = {
    		String serverId = params.serverId
    		String key = params.key
    		List metricsList = Metrics.findAllByServerIdAndKey(serverId, key, [max:50, sort:"timeStamp", order:"desc"])
    	   if(metricsList != null || metricsList.size !=0){
    		   int count = Math.min(10, metricsList.size())
    		   String[] s = new String[count]
          		for (int i = 0; i < count; i++) {
//          			s[count-i-1] = ""+ metricsList.get(i).value
          			s[i] = ""+ metricsList.get(i).value
          		}
          		 render s   
    	   } else {
    		   render "[0]"
    	   }
    		
    	}


    	def node = {
    		def cluster = Cluster.get(params.cluster)
    		String nodeName = params.node
    		ZkClient zkClient = zkService.getZkConnection(cluster.name, cluster.zkUrl);
    		List shards = zkClient.getChildren(cluster.rootNode+"/node-to-shard/"+nodeName)
    		List assignedShards = new ArrayList()
    		for (String shardName : shards) {
    			AssignedShard assignedShard = zkClient.readData(cluster.rootNode+"/node-to-shard/"+nodeName+"/"+shardName)
    			assignedShards.add(assignedShard);
			}
    		[cluster:params.cluster, node:nodeName, shards:assignedShards, charts:grailsApplication.config.katta.node.charts]
        }

    	def getNodeData = {
    		String chartName = params.chartName
    		String nodeName = params.nodeName.replaceAll(":", "")
    		List chartSources = grailsApplication.config.katta.node.charts[chartName]

            ArrayList jsonResult = new ArrayList();
            for (String sourceName : chartSources) {
            	List metricsList = Metrics.findAllByServerIdAndKey(nodeName, sourceName, [max:25, sort:"timeStamp", order:"desc"])
            	List values = new ArrayList()
            	for (Metrics metrics : metricsList) {
					values.add([metrics.timeStamp, metrics.value])
				}
            	jsonResult.add([data: values, lines:[show:true], points:[show:true], label:sourceName])
		       
	        }
            render jsonResult as JSON
        }
}
