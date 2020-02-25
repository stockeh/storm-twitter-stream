package cs535.twitter.bolt;

import java.text.BreakIterator;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitSentence extends BaseBasicBolt {

	private static final long serialVersionUID = 7378606390042918385L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		String sentence = tuple.getString( 0 );
		BreakIterator boundary = BreakIterator.getWordInstance();
		boundary.setText( sentence );
		int start = boundary.first();
		for ( int end = boundary.next(); end != BreakIterator.DONE; start =
				end, end = boundary.next() )
		{
			String word = sentence.substring( start, end );
			word = word.replaceAll( "\\s+", "" );
			if ( word.length() > 1 )
			{
				collector.emit( new Values( word ) );
			}
		}
		collector.emit( new Values( sentence ) );
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields( "word" ) );
	}
}
