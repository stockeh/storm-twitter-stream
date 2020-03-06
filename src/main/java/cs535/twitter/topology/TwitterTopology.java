package cs535.twitter.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cs535.twitter.bolt.LossyCount;
import cs535.twitter.bolt.Reporter;
import cs535.twitter.spout.TwitterSpout;

public class TwitterTopology {

	private static final Logger LOG =
			LoggerFactory.getLogger( TwitterTopology.class );

	private static final String TOPOLOGY_NAME = "twitter-count";
	private static final String SENTANCE_SPOUT_ID = "hashtag-spout";
	private static final String COUNT_BOLT_ID = "count-bold";
	private static final String REPORT_BOLT_ID = "report-bold";

	private final TopologyBuilder builder;

	private final Config conf;

	private TwitterTopology(boolean parallel) {
		builder = createTopology( parallel );
		conf = createConfig( parallel );
	}

	public static void main(String[] args) throws Exception {
		boolean parallel = false;
		boolean local = false;

		if ( args.length > 1 )
		{
			parallel = args[ 0 ].equalsIgnoreCase( "parallel" );
			local = args[ 1 ].equalsIgnoreCase( "local" );
		} else
		{
			LOG.error( "Exiting! Invalid CLI parameters." );
			System.exit( 1 );
		}

		TwitterTopology t = new TwitterTopology( parallel );

		if ( local )
		{
			LocalTopology.run( TOPOLOGY_NAME, t.conf, t.builder, 40000 * 2 );
		} else
		{
			StormSubmitter.submitTopology( TOPOLOGY_NAME, t.conf,
					t.builder.createTopology() );
		}
	}

	private Config createConfig(boolean parallel) {
		Config conf = new Config();
		if ( parallel )
		{
			conf.setNumWorkers( 3 );
		}
		return conf;
	}

	private TopologyBuilder createTopology(boolean parallel) {

		int executorTasks = 1;
		int threads = 1;
		if ( parallel )
		{
			executorTasks = 4;
			threads = 2;
		}

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout( SENTANCE_SPOUT_ID, new TwitterSpout() );
		builder.setBolt( COUNT_BOLT_ID, new LossyCount(), threads )
				.setNumTasks( executorTasks )
				.fieldsGrouping( SENTANCE_SPOUT_ID, new Fields( "hash" ) );
		builder.setBolt( REPORT_BOLT_ID, new Reporter() )
				.globalGrouping( COUNT_BOLT_ID );

		LOG.info( "Topology name: " + TOPOLOGY_NAME );

		return builder;
	}

}
