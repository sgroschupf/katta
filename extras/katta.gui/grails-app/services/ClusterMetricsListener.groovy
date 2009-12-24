import java.io.Serializable;

import org.I0Itec.zkclient.IZkDataListener;

import net.sf.katta.util.ZkConfiguration.PathDef;
import net.sf.katta.monitor.MetricsRecord 
import net.sf.katta.protocol.ConnectedComponent;
import net.sf.katta.protocol.IAddRemoveListener;
import net.sf.katta.protocol.InteractionProtocol;


class ClusterMetricsListener implements ConnectedComponent,IAddRemoveListener, IZkDataListener{
	
	private final Cluster _cluster;
	private final InteractionProtocol _protocol;
	
	public ClusterMetricsListener(Cluster cluster, InteractionProtocol protocol) {
		_cluster = cluster;
		_protocol = protocol;
	}
	
	void start(){
		_protocol.registerComponent(this);
		List<String> nodes= _protocol.registerChildListener(this, PathDef.NODE_METRICS, this);
		for (String node : nodes) {
			_protocol.registerDataListener(this, PathDef.NODE_METRICS, node, this);
		}
	}
	
	void stop(){
		_protocol.unregisterComponent(this);
	}
	
	void added(String name) {
		_protocol.registerDataListener(this, PathDef.NODE_METRICS, name, this);
	}
	
	void removed(String name) {
		_protocol.unregisterDataListener(this, PathDef.NODE_METRICS, name, this);
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
	
	public void handleDataDeleted(String dataPath) throws Exception {
		// remove listener is handled #removed() method
	}
	
	void reconnect() {
		//nothing todo
	}
	
	void disconnect() {
		//nothing todo			  
	}
	
}