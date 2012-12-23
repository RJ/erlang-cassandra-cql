## Cassandra CQL Client

* Woefully incomplete and untested
* Speaks the [Cassandra native binary protocol](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol.spec#L167) for CQL queries
* Does not use Thrift
* You'll need to set ````start_native_transport: true```` in
  ````cassandra.yaml````


### Quickstart

````
rebar compile
erl -pa ebin -boot start_sasl

{ok, Pid} = ecql_connection:start_link().
ecql_connection:q(Pid, <<"USE my_keyspace">>).
ecql_connection:q(Pid, <<"SELECT * from my_table LIMIT 3">>, one).
````




