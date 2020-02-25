package cs535.twitter.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import cs535.twitter.util.Properties;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSpout extends BaseRichSpout {

	private static final long serialVersionUID = 6549071650320465459L;

	private static final Logger LOG =
			LoggerFactory.getLogger( TwitterSpout.class );

	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue = null;
	private TwitterStream twitterStream;

	@Override
	public void open(Map<String, Object> conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>( 1000 );
		this.collector = collector;

		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				queue.offer( status );
			}

			@Override
			public void onException(Exception ex) {}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {}

			@Override
			public void onStallWarning(StallWarning warning) {}

		};

		twitterStream = new TwitterStreamFactory(
				new ConfigurationBuilder().setJSONStoreEnabled( true ).build() )
						.getInstance();

		LOG.info( "OAUTH_CONSUMER_KEY: " + Properties.OAUTH_CONSUMER_KEY );
		LOG.info(
				"OAUTH_CONSUMER_SECRET: " + Properties.OAUTH_CONSUMER_SECRET );

		LOG.info( "OAUTH_ACCESS_TOKEN: " + Properties.OAUTH_ACCESS_TOKEN );
		LOG.info( "OAUTH_ACCESS_TOKEN_SECRET: "
				+ Properties.OAUTH_ACCESS_TOKEN_SECRET );


		twitterStream.setOAuthConsumer( Properties.OAUTH_CONSUMER_KEY,
				Properties.OAUTH_CONSUMER_SECRET );

		twitterStream.setOAuthAccessToken(
				new AccessToken( Properties.OAUTH_ACCESS_TOKEN,
						Properties.OAUTH_ACCESS_TOKEN_SECRET ) );

		twitterStream.addListener( listener );
		twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		Status status = queue.poll();
		if ( status == null )
		{
			Utils.sleep( 50 );
		} else
		{
			for ( HashtagEntity e : status.getHashtagEntities() )
			{
				collector.emit( new Values( e.getText() ) );
			}
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism( 1 );
		return conf;
	}

	@Override
	public void ack(Object id) {}

	@Override
	public void fail(Object id) {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare( new Fields( "text" ) );
	}

}
