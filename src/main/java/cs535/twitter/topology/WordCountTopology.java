package cs535.twitter.topology;

import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cs535.twitter.bolt.SplitSentence;
import cs535.twitter.bolt.WordCount;
import cs535.twitter.spout.RandomSentenceSpout;

public class WordCountTopology extends ConfigurableTopology {

	private static final Logger LOG =
			LoggerFactory.getLogger( WordCountTopology.class );

	public static void main(String[] args) throws Exception {
		ConfigurableTopology.start( new WordCountTopology(), args );
	}

	// Entry point for the topology
	@Override
	protected int run(String[] args) {

		String topologyName = "word-count";
		if ( args.length >= 1 )
		{
			topologyName = args[ 0 ];
		}
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout( "spout", new RandomSentenceSpout(), 5 );
		// Add the SplitSentence bolt, with a name of 'split'
		// and parallelism hint of 8 executors
		// shufflegrouping subscribes to the spout, and equally distributes
		// tuples (sentences) across instances of the SplitSentence bolt
		builder.setBolt( "split", new SplitSentence(), 8 )
				.shuffleGrouping( "spout" );
		// Add the counter, with a name of 'count'
		// and parallelism hint of 12 executors
		// fieldsgrouping subscribes to the split bolt, and
		// ensures that the same word is sent to the same instance (group by
		// field 'word')
		builder.setBolt( "count", new WordCount(), 12 ).fieldsGrouping( "split",
				new Fields( "word" ) );

		LOG.info( "Topology name: " + topologyName );

		return super.submit( topologyName, super.conf, builder );
	}
}
