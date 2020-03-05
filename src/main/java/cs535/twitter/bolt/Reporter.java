package cs535.twitter.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
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
import org.apache.storm.tuple.Tuple;
import cs535.twitter.util.Properties;
import cs535.twitter.util.Utilities;

public class Reporter extends BaseRichBolt {

	private static final long serialVersionUID = -6466276910345741034L;

	private static final Logger LOG = LogManager.getLogger( Reporter.class );

	private final Map<String, Long> counts = new HashMap<>();

	private BufferedWriter buffer;

	@Override
	public void prepare(Map<String, Object> topoConf, TopologyContext context,
			OutputCollector collector) {
		try
		{
			buffer = new BufferedWriter(
					new FileWriter( Properties.LOG_FILE, true ) );
		} catch ( IOException e )
		{
			LOG.error( e.getMessage(), e );
			e.printStackTrace();
		}
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
			write();
		} else
		{
			counts.put( input.getStringByField( "word" ),
					input.getLongByField( "count" ) );
		}

	}

	private void write() {
		int size = counts.size() > 100 ? 100 : counts.size();
		if ( size == 0 )
		{
			return;
		}
		StringBuilder sb = new StringBuilder( Instant.now().toString() );
		sb.append( " ::: " );

		Map<String, Long> output = Utilities.sortMapDecending( counts, size );

		counts.clear();


		for ( Entry<String, Long> entry : output.entrySet() )
		{
			sb.append( entry.getKey() ).append( "(" ).append( entry.getValue() )
					.append( "), " );
		}
		LOG.info( sb.toString() + "\n\n" );
		try
		{
			buffer.write( sb.toString() + "\n\n" );
			buffer.flush();
		} catch ( IOException e )
		{
			LOG.error( e.getMessage(), e );
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

}
