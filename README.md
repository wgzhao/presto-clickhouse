# Trino/PrestoSQL ClickHouse Connector

The repo has [merged into offical repo](https://github.com/trinodb/trino/pulls/4909), It will be ship on since version 353.

If you are using version 350 or earlier (PrestoSQL), you can get it working by executing the following code:

```shell
wget -O /tmp/presto-clickhouse-352.zip \
 https://github.com/wgzhao/presto-clickhouse/releases/download/350/presto-clickhouse-350.zip
unzip -q -o /tmp/presto-clickhouse-352.zip -d /usr/lib/presto/plugin
```

If you are using version 352 (Trino), you can get it working by executing the following code:

```shell
wget -O /tmp/trino-clickhouse-352.zip \
 https://github.com/wgzhao/presto-clickhouse/releases/download/352/trino-clickhouse-352.zip
unzip -q -o /tmp/trino-clickhouse-352.zip -d /usr/lib/trino/plugin
```

## Connection Configuration

Create new properties file inside etc/catalog dir:

```ini
connector.name=clickhouse
connection-url=jdbc:clickhouse://ip:port/database
connection-user=myuser
connection-password=
```

your can get detailed in the [documentation](https://github.com/trinodb/trino/blob/master/docs/src/main/sphinx/connector/clickhouse.rst).