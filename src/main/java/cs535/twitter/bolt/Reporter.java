package cs535.twitter.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import cs535.twitter.util.Properties;

public class Reporter extends BaseRichBolt {

	private static final long serialVersionUID = -6466276910345741034L;

	private static final Logger LOG = LogManager.getLogger( Reporter.class );

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
	public void execute(Tuple input) {
		try
		{
			buffer.write( input.getStringByField( "time" ) + " "
					+ input.getStringByField( "output" ) + "\n\n" );
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
