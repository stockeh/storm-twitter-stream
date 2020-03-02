package cs535.twitter.topology;

import org.apache.storm.Config;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cs535.twitter.bolt.SplitSentence;
import cs535.twitter.bolt.LossyCount;
import cs535.twitter.spout.RandomSentenceSpout;

public class WordCountTopology extends ConfigurableTopology {

	private static final Logger LOG =
			LoggerFactory.getLogger( WordCountTopology.class );

	private static final String TOPOLOGY_NAME = "word-count";
	private static final String SENTANCE_SPOUT_ID = "sentance-spout";
	private static final String SPLIT_BOLT_ID = "split-bold";
	private static final String COUNT_BOLT_ID = "count-bold";


	public static void main(String[] args) throws Exception {

		if ( args.length > 0 )
		{
			if ( args[ 0 ].equalsIgnoreCase( "local" ) )
			{
				LocalTopology.run( TOPOLOGY_NAME, new Config(),
						new WordCountTopology().createTopology(), 30000 );
			}
		} else
		{
			ConfigurableTopology.start( new WordCountTopology(), args );
		}
	}

	private TopologyBuilder createTopology() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout( SENTANCE_SPOUT_ID, new RandomSentenceSpout(), 5 );
		builder.setBolt( SPLIT_BOLT_ID, new SplitSentence(), 8 )
				.shuffleGrouping( SENTANCE_SPOUT_ID );
		builder.setBolt( COUNT_BOLT_ID, new LossyCount(), 12 )
				.fieldsGrouping( SPLIT_BOLT_ID, new Fields( "word" ) );

		LOG.info( "Topology name: " + TOPOLOGY_NAME );

		return builder;
	}

	@Override
	protected int run(String[] args) {
		return super.submit( TOPOLOGY_NAME, super.conf, createTopology() );
	}

}
