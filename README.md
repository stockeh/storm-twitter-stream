# storm-twitter-stream
Detecting the Most Popular Topics from Live Twitter Message Streams using the Lossy Counting Algorithm with Apache Storm

Execution Steps

1. Start Storm with local bashrc configuration
```
$ stormstart
```

2. Package and run with Storm
```
$ mvn package
$ storm jar target/storm-twitter-stream-0.0.1-SNAPSHOT.jar cs535.twitter.topology.WordCountTopology
```
3. View the output logs
```
$ tail -20 $SPARK_HOME/logs/workers-artifacts/...
```
