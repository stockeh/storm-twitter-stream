package cs535.twitter.topology;

import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cs535.twitter.bolt.SplitSentence;
import cs535.twitter.bolt.WordCount;
import cs535.twitter.spout.RandomSentenceSpout;

public class TwitterTopology extends ConfigurableTopology {

	private static final Logger LOG =
			LoggerFactory.getLogger( TwitterTopology.class );

	public static void main(String[] args) {
		ConfigurableTopology.start( new TwitterTopology(), args );
	}

	@Override
	protected int run(String[] args) throws Exception {

		if ( args.length < 4 )
		{
			LOG.error( "Usage: " );
			System.exit( 1 );
		}

		String consumerKey = args[ 0 ];
		String consumerSecret = args[ 1 ];

		String accessToken = args[ 2 ];
		String accessTokenSecret = args[ 3 ];

		TopologyBuilder builder = new TopologyBuilder();
		// builder.setSpout( "spout", new RandomSentenceSpout(), 5 );

		// builder.setBolt( "split", new SplitSentence(), 8 )
		// 		.shuffleGrouping( "spout" );

		// builder.setBolt( "count", new WordCount(), 12 ).fieldsGrouping( "split",
		//		new Fields( "word" ) );

		return super.submit( "twitter-stream", super.conf, builder );
	}

}
