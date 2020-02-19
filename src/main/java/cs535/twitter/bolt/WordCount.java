package cs535.twitter.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import cs535.twitter.util.Utilities;

public class WordCount extends BaseBasicBolt {

	private static final long serialVersionUID = -7619769952927707443L;

	private static final Logger LOG = LogManager.getLogger( WordCount.class );

	private final Map<String, Integer> counts = new HashMap<String, Integer>();
	private final int emitFrequency;

	public WordCount() {
		emitFrequency = 10;
	}

	public WordCount(int frequency) {
		emitFrequency = frequency;
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put( Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency );
		return conf;
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		if ( tuple.getSourceComponent().equals( Constants.SYSTEM_COMPONENT_ID )
				&& tuple.getSourceStreamId()
						.equals( Constants.SYSTEM_TICK_STREAM_ID ) )
		{
			String total = "-----------" + counts.size() + " total-----------";
			LOG.info( total );
			collector.emit( new Values( total, -1 ) );
			
			Map<String, Integer> output =
					Utilities.sortMapByValue( counts, true );
			counts.clear();
			int items = 0;
			for ( Entry<String, Integer> entry : output.entrySet() )
			{
				collector
						.emit( new Values( entry.getKey(), entry.getValue() ) );
				LOG.info( "Word: " + entry.getKey() + ", Count: "
						+ entry.getValue() );
				if ( ++items == 100 )
				{
					break;
				}
			}

		} else
		{
			String word = tuple.getString( 0 );
			Integer count = counts.get( word );
			if ( count == null )
			{
				count = 0;
			}
			count++;
			counts.put( word, count );
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields( "word", "count" ) );
	}
}
