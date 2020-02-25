package cs535.twitter.topology;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LocalTopology {

	private static final Logger LOG =
			LoggerFactory.getLogger( LocalTopology.class );

	/**
	 * Run the topology in the local cluster
	 * 
	 * @param topology
	 * @param name
	 */
	public static final void run(String name, Config conf,
			TopologyBuilder topology, long timeout) {
		LocalCluster cluster = null;
		try
		{
			cluster = new LocalCluster();

			cluster.submitTopology( name, conf, topology.createTopology() );

			Utils.sleep( timeout );

			final KillOptions killOptions = new KillOptions();
			killOptions.set_wait_secs( 0 );

			cluster.killTopologyWithOpts( name, killOptions );

			while ( topologyExists( cluster, name ) )
			{
				Utils.sleep( 1000 );
			}

		} catch ( Exception e )
		{
			LOG.error( e.getMessage() );
			e.printStackTrace();
		} finally
		{
			Utils.sleep( 5000 );
			cluster.shutdown();
			System.exit( 0 );
		}
	}

	/**
	 * 
	 * Referenced from this SO post
	 * 
	 * stackoverflow.com/questions/45176886/error-running-storm-topology-locally/47396504
	 * 
	 * @param cluster
	 * @return
	 * @throws TException
	 */
	private static final boolean topologyExists(LocalCluster cluster,
			String name) throws TException {

		// list all the topologies on the local cluster
		final List<TopologySummary> topologies =
				cluster.getClusterInfo().get_topologies();

		// search for a topology with the topologyName
		if ( null != topologies && !topologies.isEmpty() )
		{
			final List<TopologySummary> collect = topologies.stream()
					.filter( p -> p.get_name().equals( name ) )
					.collect( Collectors.toList() );
			if ( null != collect && !collect.isEmpty() )
			{
				return true;
			}
		}
		return false;
	}

}
