# flink-table-custom
主要是测试和自定义扩展flink sql的一些功能。
1. flink-jdbc-connector
	- 可以指定table column，通过此column实现并发读取的功能
	- 可以指定谓词下推column，将`>,>=,<,<=,=`下推到数据库中
1. flink-clickhouse-connector
	- 可以通过扩展`PartitionerFactory`接口实现自定义shardingkey
