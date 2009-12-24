import net.sf.katta.util.ZkConfiguration;

import net.sf.katta.protocol.InteractionProtocol;
import java.util.List;
import org.I0Itec.zkclient.ZkClient;


class ZkService {
	
	private Map<String, InteractionProtocol> _protocolsByClusterName = new HashMap<String, InteractionProtocol>();
	private List<ClusterMetricsListener> _metricsListener= new ArrayList<String>();
	boolean transactional = true
	
	def InteractionProtocol getProtocol(Cluster cluster){
		InteractionProtocol protocol = _protocolsByClusterName.get(cluster.getName());
		if(protocol==null){
			ZkConfiguration zkConf = new ZkConfiguration();
			zkConf.setZKRootPath(cluster.getRootNode());
			zkConf.setZKServers(cluster.getZkUrl());
			ZkClient zkClient = new ZkClient(zkConf.getZKServers());
			protocol =  new InteractionProtocol(zkClient, zkConf);
			_protocolsByClusterName.put(cluster.getName(), protocol);
		}
		return protocol;
	}
	
	def startMetricsListening(){
		List clustersFromDB =  Cluster.findAll()
		for (Cluster cluster : clustersFromDB) {
			startMetricsListening cluster;
		}
	}
	
	def startMetricsListening(Cluster cluster){
		System.out.println("Subscribing to cluster: " + cluster.name);
		InteractionProtocol protocol = getProtocol(cluster);
		ClusterMetricsListener metricsListener = new ClusterMetricsListener(cluster, protocol);
		_metricsListener.add(metricsListener);
		metricsListener.start();
	}
	
	def stopMetricsListening(){
		for (ClusterMetricsListener listener : _metricsListener) {
			listener.stop();
		}
		_metricsListener.clear();
	}
	
	
}
