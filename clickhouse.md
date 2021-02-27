# ClickHouse Connector

The ClickHouse connector allows querying tables in an external [Yandex
ClickHouse](https://clickhouse.tech/) instance. This can be used to join
data between different systems like ClickHouse and Hive, or between two
different ClickHouse instances.

## Configuration

To configure the ClickHouse connector, create a catalog properties
file`etc/catalog/clickhouse.properties`, replace the connection
properties as needed for your setup:

```ini
connector.name=clickhouse
connection-url=jdbc:clickhouse:host1:8123:/default
connection-user=default
connection-password=
```

## Multiple ClickHouse servers

The ClickHouse connector can\'t access more than one database using a
single catalog. If you have multiple ClickHouse servers you need to
configure one catalog for each instance. To add another catalog:

-   Add another properties file to `etc/catalog`
-   Save it with a different name that ends in `.properties`

For example, if you name the property file `sales.properties`, Presto
uses the configured connector to create a catalog named `sales`.

## Querying ClickHouse

The ClickHouse connector provides a schema for every ClickHouse
*database*. run `SHOW SCHEMAS` to see the available ClickHouse
databases:

```sql
SHOW SCHEMAS FROM clichouse;
```


If you have a ClickHouse database named `web`, run `SHOW TABLES` to view
the tables in this database:

```sql
SHOW TABLES FROM clichouse.web;
```


Run `DESCRIBE` or `SHOW COLUMNS` to list the columns in the `clicks`
table in the `web` databases:

```sql
DESCRIBE clichouse.web.clicks;
SHOW COLUMNS FROM clichouse.web.clicks;
```

Run `SELECT` to access the `clicks` table in the `web` database:

```sql
SELECT * FROM clichouse.web.clicks;
```

Note:

If you used a different name for your catalog properties file, use that
catalog name instead of `ClickHouse` in the above examples.


## ClickHouse Connector Limitations

The following SQL statements aren't supported:

- `grant`
- `revoke`
- `show-grants`
- `show-roles`
- `show-role-grants`
