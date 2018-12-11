Similar to the kafka-connect-jdbc connector, but:
* JDBC dialect is always Oracle
* only one view is ever queried
* this will only ever have a SourceConnector, no sinks