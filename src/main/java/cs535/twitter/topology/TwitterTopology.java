package cs535.twitter.topology;

import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cs535.twitter.bolt.SplitSentence;
import cs535.twitter.bolt.WordCount;
import cs535.twitter.spout.TwitterSpout;

public class TwitterTopology extends ConfigurableTopology {

	private static final Logger LOG =
			LoggerFactory.getLogger( TwitterTopology.class );

	private static final String TOPOLOGY_NAME = "twitter-count";
	private static final String SENTANCE_SPOUT_ID = "sentance-spout";
	private static final String SPLIT_BOLT_ID = "split-bold";
	private static final String COUNT_BOLT_ID = "count-bold";

	public static void main(String[] args) throws Exception {

		if ( args.length > 0 )
		{
			if ( args[ 0 ].equalsIgnoreCase( "local" ) )
			{
				LocalTopology.run( new TwitterTopology().createTopology( args ),
						TOPOLOGY_NAME, 60000 * 1 );
			}
		} else
		{
			ConfigurableTopology.start( new TwitterTopology(), args );
		}
	}

	private TopologyBuilder createTopology(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout( SENTANCE_SPOUT_ID, new TwitterSpout(), 1 );
		builder.setBolt( SPLIT_BOLT_ID, new SplitSentence(), 8 )
				.shuffleGrouping( SENTANCE_SPOUT_ID );
		builder.setBolt( COUNT_BOLT_ID, new WordCount(), 1 )
				.fieldsGrouping( SPLIT_BOLT_ID, new Fields( "word" ) );

		LOG.info( "Topology name: " + TOPOLOGY_NAME );

		return builder;
	}

	@Override
	protected int run(String[] args) {
		return super.submit( TOPOLOGY_NAME, super.conf,
				createTopology( args ) );
	}

}
