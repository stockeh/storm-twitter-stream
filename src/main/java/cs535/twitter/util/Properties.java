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

}
