import net.sf.katta.monitor.MetricsRecord;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;


class ZkService  implements IZkChildListener, IZkDataListener {
	
	private HashMap<String, ZkClient> _connections = new HashMap<String, ZkClient>();
	boolean transactional = true
	
	
	def ZkClient getZkConnection(String name, String zkUrlString){
		if(_connections.containsKey(name)){
			return _connections.get(name);
		} else {
			ZkClient zkClient =  new ZkClient(zkUrlString);
			_connections.put(name, zkClient);
			return zkClient;
		}
	}
	
	def subScribeMetricsUpdates(){
		List clustersFromDB =  Cluster.findAll()
		for (Cluster cluster : clustersFromDB) {
			System.out.println("Subscribing to cluster: " + cluster.name);
			ZkClient zkClient = getZkConnection (cluster.name, cluster.zkUrl)
			String zkMetricsPath = cluster.rootNode + "/server-metrics"
			zkClient.subscribeChildChanges(zkMetricsPath, this);
			List<String> children = zkClient.getChildren(zkMetricsPath);
			subscribeDataUpdates(zkClient, zkMetricsPath, children);
		}
	}
	
	private void subscribeDataUpdates(ZkClient zkClient, String parentPath, List<String> currentChilds) {
		for (String childName : currentChilds) {
			zkClient.subscribeDataChanges(parentPath + "/" + childName, this);
		}
	}
	
	public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
		// unfortunately we do not know which of the clusters has new nodes, so we just have to re subscribe to everything
		subScribeMetricsUpdates()
	}
	
	
	
	public void handleDataChange(String dataPath, Serializable data) throws Exception {
		MetricsRecord r = (MetricsRecord)data
		List records = r.getRecords()
		String serverId = r._serverId.replace(":","")
		for (MetricsRecord.Record record : records) {
			def m = new Metrics(serverId:serverId, key:record._key, value:record._value, timeStamp:record._timeStamp)
				m.save()
			}
		List toDelete = Metrics.findAllByTimeStampLessThan(System.currentTimeMillis() - 1000 * 60 * 2) // we only keep the last 2 minutes for now....
		for (Metrics m : toDelete) {
			m.delete();
		}
	}
	
	public void handleNewSession() throws Exception {
	}
	
	public void handleDataDeleted(String dataPath) throws Exception {
	}


	
 
	
}
