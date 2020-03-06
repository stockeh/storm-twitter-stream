package cs535.twitter.util;

/**
 * 
 * @author stock
 *
 */
public interface Properties {

	final String CONF_NAME = "application.properties";

	final String OAUTH_CONSUMER_KEY =
			Configurations.getInstance().getProperty( "oauth.consumer.key" );

	final String OAUTH_CONSUMER_SECRET =
			Configurations.getInstance().getProperty( "oauth.consumer.secret" );

	final String OAUTH_ACCESS_TOKEN =
			Configurations.getInstance().getProperty( "oauth.access.token" );

	final String OAUTH_ACCESS_TOKEN_SECRET = Configurations.getInstance()
			.getProperty( "oauth.access.token.secret" );

	final String LOG_FILE =
			Configurations.getInstance().getProperty( "log.file" );

	final Double EPSILON = Double.parseDouble(
			Configurations.getInstance().getProperty( "epsilon", "0.20" ) );

	final Double THRESHOLD = Double.parseDouble(
			Configurations.getInstance().getProperty( "threshold", "0.20" ) );

	final String SEPERATOR = "\t";

}
