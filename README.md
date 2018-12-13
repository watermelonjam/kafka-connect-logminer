Similar to the kafka-connect-jdbc connector, but:
* JDBC dialect is always Oracle
* only one view is ever queried
* this will only ever have a SourceConnector, no sinks

Running the DatabaseTest tests requires the correct maven build properties to be set, as well as an Oracle JDBC driver dependency declared in an external profile.