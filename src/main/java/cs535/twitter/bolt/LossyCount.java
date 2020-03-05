package cs535.twitter.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import cs535.twitter.util.Properties;
import cs535.twitter.util.Utilities;

public class LossyCount extends BaseRichBolt {

	private static final long serialVersionUID = -7619769952927707443L;

	private static final Logger LOG = LogManager.getLogger( LossyCount.class );

	private final Map<String, Item> counts = new HashMap<>();
	private OutputCollector collector;
	private int capacity;
	private int bucket;
	private long items;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context,
			OutputCollector collector) {
		this.capacity = ( int ) ( 1 / Properties.EPSILON );
		this.bucket = 1;
		this.items = 0;
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields( "word", "count" ) );
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		int emitFrequency = 10;
		Config conf = new Config();
		conf.put( Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency );
		return conf;
	}

	@Override
	public void execute(Tuple input) {
		if ( input.getSourceComponent().equals( Constants.SYSTEM_COMPONENT_ID )
				&& input.getSourceStreamId()
						.equals( Constants.SYSTEM_TICK_STREAM_ID ) )
		{
			forward();
		} else
		{
			if ( ++items % capacity == 0 )
			{
				// process entire bucket and then run delete phase.
				delete();
				++bucket;
			}
			insert( input );
		}
	}

	private void insert(Tuple input) {
		String s = input.getStringByField( "hash" );
		Item item = counts.get( s );
		if ( item == null )
		{
			item = new Item( bucket - 1 );
		}
		item.increment();
		counts.put( s, item );
	}

	private void delete() {
		counts.values().removeIf( value -> value.deconstruct( bucket ) );
	}

	private void forward() {
		int size = counts.size() > 100 ? 100 : counts.size();
		if ( size == 0 )
		{
			return;
		}
		Map<String, Item> output = Utilities.sortMapDecending( counts, size );
		StringBuilder sb = new StringBuilder();
		for ( Entry<String, Item> entry : output.entrySet() )
		{
			// sb.append( entry.getKey() ).append( "_" ).append( bucket )
			// .append( "_" ).append( entry.getValue().toString() )
			// .append( " " );
			collector.emit(
					new Values( entry.getKey(), entry.getValue().frequency ) );
		}
		// LOG.info( sb.toString() );
	}

	private final static class Item implements Comparable<Item> {

		private final int delta;
		private long frequency = 0L;

		private Item(int delta) {
			this.delta = delta;
		}

		private void increment() {
			frequency++;
		}

		private boolean deconstruct(int currentBucket) {
			return frequency + delta <= currentBucket;
		}

		@Override
		public int compareTo(Item o) {
			return Long.compare( this.frequency, o.frequency );
		}

		@Override
		public String toString() {
			return "( " + delta + ", " + frequency + " )";
		}
	}


}
