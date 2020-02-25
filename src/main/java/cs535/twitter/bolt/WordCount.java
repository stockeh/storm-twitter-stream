package cs535.twitter.bolt;

import java.time.Instant;
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

	private final Map<String, Long> counts = new HashMap<>();

	@Override
	public Map<String, Object> getComponentConfiguration() {
		int emitFrequency = 10;
		
		Config conf = new Config();
		conf.put( Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency );
		return conf;
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if ( input.getSourceComponent().equals( Constants.SYSTEM_COMPONENT_ID )
				&& input.getSourceStreamId()
						.equals( Constants.SYSTEM_TICK_STREAM_ID ) )
		{
			int size = counts.size() > 100 ? 100 : counts.size();
			if ( size == 0 )
			{
				return;
			}

			Map<String, Long> output = Utilities.sortMapByValue( counts, true );
			counts.clear();

			StringBuilder sb = new StringBuilder();

			int items = 0;
			for ( Entry<String, Long> entry : output.entrySet() )
			{
				sb.append( entry.getKey() ).append( "(" )
						.append( entry.getValue() ).append( "), " );

				if ( ++items == 100 )
				{
					break;
				}
			}
			collector.emit( new Values( sb.toString(), Instant.now().toString() ) );
			LOG.info( sb.toString() );
		} else
		{
			String word = input.getStringByField( "hash" );
			Long count = counts.get( word );
			if ( count == null )
			{
				count = 0L;
			}
			count++;
			counts.put( word, count );
		}
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields( "output", "time" ) );
	}
}
