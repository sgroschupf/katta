import java.util.ArrayList;
import java.util.List;

import net.sf.katta.protocol.InteractionProtocol;
import net.sf.katta.protocol.metadata.IndexMetaData;
import net.sf.katta.operation.master.AbstractIndexOperation;
import net.sf.katta.protocol.metadata.MasterMetaData;
import net.sf.katta.protocol.metadata.NodeMetaData;

import grails.converters.JSON

class DashboardController {
	
	def zkService
	
	def index = {
		def cluster = Cluster.get(params.id)
		InteractionProtocol protocol = zkService.getProtocol(cluster); 
		//master
		MasterMetaData master = protocol.getMasterMD();
		
		// nodes
		List nodeNames = protocol.getKnownNodes()
		List liveNames = protocol.getLiveNodes()
		List nodes = new ArrayList()
		for (String nodeName : nodeNames) {
			NodeMetaData meta = protocol.getNodeMD(nodeName)
			int assigned = protocol.getNodeShards(nodeName).size()
			String state = "DISCONNECTED"
			if (liveNames.contains(nodeName)) {
				state = "CONNECTED"
			}
			nodes.add([id:meta.name.replace(":",""), name:meta.name, state:state, started:meta.startTimeAsDate, assigned:assigned])
		}
		
		// indices
		List indexNames = protocol.getIndices();
		List indexes = new ArrayList()
		for (String indexName : indexNames) {
			IndexMetaData meta = protocol.getIndexMD(indexName);
			String state = "DEPLOYED"
			if (meta.hasDeployError()) {
				state = "ERROR"
			}
			indexes.add([name:meta.name, path:meta.path, state:state,repLevel:meta.replicationLevel]);
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
		InteractionProtocol protocol = zkService.getProtocol(cluster);
		List<String> shards = new ArrayList<String>(protocol.getNodeShards(nodeName))
		List<String> indexNames = new ArrayList<String>()
		List<String> shardPathes = new ArrayList<String>()
		for (String shardName : shards) {
			String indexName = AbstractIndexOperation.getIndexNameFromShardName(shardName);
			IndexMetaData indexMD = protocol.getIndexMD(indexName)
			indexNames.add(indexName)
			shardPathes.add(indexMD.getShardPath(shardName))
		}
		[cluster:params.cluster, node:nodeName, shards:shards, shardIndices:indexNames, shardPathes:shardPathes , charts:grailsApplication.config.katta.node.charts]
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
