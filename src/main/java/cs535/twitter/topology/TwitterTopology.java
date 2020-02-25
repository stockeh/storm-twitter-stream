package cs535.twitter.topology;

import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cs535.twitter.bolt.WordCount;
import cs535.twitter.spout.TwitterSpout;

public class TwitterTopology extends ConfigurableTopology {

	private static final Logger LOG =
			LoggerFactory.getLogger( TwitterTopology.class );

	private static final String TOPOLOGY_NAME = "twitter-count";
	private static final String SENTANCE_SPOUT_ID = "sentance-spout";
	private static final String COUNT_BOLT_ID = "count-bold";

	public static void main(String[] args) throws Exception {

		if ( args.length > 0 )
		{
			if ( args[ 0 ].equalsIgnoreCase( "local" ) )
			{
				LocalTopology.run( new TwitterTopology().createTopology( args ),
						TOPOLOGY_NAME, 40000 * 2 );
			}
		} else
		{
			ConfigurableTopology.start( new TwitterTopology(), args );
		}
	}

	private TopologyBuilder createTopology(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout( SENTANCE_SPOUT_ID, new TwitterSpout(), 3 );
		builder.setBolt( COUNT_BOLT_ID, new WordCount() )
				.fieldsGrouping( SENTANCE_SPOUT_ID, new Fields( "text" ) );

		LOG.info( "Topology name: " + TOPOLOGY_NAME );

		return builder;
	}

	@Override
	protected int run(String[] args) {
		return super.submit( TOPOLOGY_NAME, super.conf,
				createTopology( args ) );
	}

}
