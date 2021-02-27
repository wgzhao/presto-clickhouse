/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.dateColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.dateWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Integer.min;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;

public class ClickHouseClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(ClickHouseClient.class);
    public static final String ESCAPE_CHARACTER = "\"";

    @Inject
    public ClickHouseClient(BaseJdbcConfig config, ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        super(config, ESCAPE_CHARACTER, connectionFactory);
    }

    @Override
    public JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String tableName)
            throws SQLException
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();

        JdbcIdentity identity = JdbcIdentity.from(session);
        if (!getSchemaNames(session).contains(schemaTableName.getSchemaName())) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(session)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());

            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            Map<String, Object> tableProperties = tableMetadata.getProperties();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();

            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                String typeStatement = toWriteMapping(session, column.getType()).getDataType();
                if (column.isNullable()) {
                    typeStatement = "Nullable(" + typeStatement + ")";
                }
                columnList.add(format("%s %s", columnName, typeStatement));
            }

            ImmutableList.Builder<String> tableOptions = ImmutableList.builder();
            ClickHouseTableProperties.getEngine(tableProperties)
                    .ifPresent(value -> tableOptions.add("ENGINE =" + value));

            String sql = format("CREATE TABLE %s.%s (%s) %s", remoteSchema, tableName, join(", ", columnList.build()),
                    join(", ", tableOptions.build()));
            execute(connection, sql);

            return new JdbcOutputTableHandle(
                    catalog,
                    null,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.empty(),
                    tableName);
        }
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        execute(session, "CREATE DATABASE " + quoted(schemaName));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        execute(session, "DROP DATABASE " + quoted(schemaName));
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        // ClickHouse maps their "database" to SQL catalogs and does not have schemas
        DatabaseMetaData metadata = connection.getMetaData();
        return metadata.getTables(
                schemaName.orElse(null),
                null,
                escapeNamePattern(tableName, metadata.getSearchStringEscape()).orElse(null),
                new String[] {"TABLE", "VIEW"});
    }
//
//    @Override
//    protected String getTableSchemaName(ResultSet resultSet)
//            throws SQLException
//    {
//        // ClickHouse uses catalogs instead of schemas
//        log.info("get table schema name: " + resultSet.getString("TABLE_CAT"));
//        return resultSet.getString("TABLE_CAT");
//    }

    @Override
    public void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn,
            String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(session)) {
            DatabaseMetaData metadata = connection.getMetaData();
            if (metadata.storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String sql = format("ALTER TABLE %s RENAME COLUMN %s TO %s ",
                    quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()),
                    quoted(jdbcColumn.getColumnName()), quoted(newColumnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName,
            SchemaTableName newTable)
    {
        String sql = format("rename table %s.%s to %s.%s", schemaName, tableName, newTable.getSchemaName(),
                newTable.getTableName());
        execute(session, sql);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection,
            JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        if (jdbcTypeName.equals("UInt8")) {
            return Optional.of(booleanColumnMapping());
        }
        if (jdbcTypeName.equals("Int8")) {
            return Optional.of(tinyintColumnMapping());
        }
        if (jdbcTypeName.equals("Int16")) {
            return Optional.of(smallintColumnMapping());
        }
        if (jdbcTypeName.equals("Int32")) {
            return Optional.of(integerColumnMapping());
        }
        if (jdbcTypeName.equals("Int64") || jdbcTypeName.equals("bigint")) {
            return Optional.of(bigintColumnMapping());
        }
        if (jdbcTypeName.equals("Float32")) {
            return Optional.of(realColumnMapping());
        }
        if (jdbcTypeName.equals("Float64")) {
            return Optional.of(doubleColumnMapping());
        }
        if (typeHandle.getJdbcType() == Types.DECIMAL) {
            int precision = min(typeHandle.getColumnSize().get(), Decimals.MAX_PRECISION);
            int decimalDigits = typeHandle.getDecimalDigits()
                    .orElseThrow(() -> new IllegalStateException("decimal digits not present"));
            int scale = min(decimalDigits, getDecimalDefaultScale(session));
            return Optional.of(decimalColumnMapping(createDecimalType(precision, scale),
                    getDecimalRoundingMode(session)));
        }
        if (jdbcTypeName.equals("String")) {
            return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
        }
        if (jdbcTypeName.equals("Date")) {
            return Optional.of(dateColumnMapping());
        }
        if (jdbcTypeName.equals("DateTime")) {
            // clickhouse not implemented for type=class java.time.LocalDateTime
            // TODO replace it using timestamp relative function after clickhouse adds support for LocalDateTime
            return Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
        }
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }
        // unknown type ?
        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("UInt8", booleanWriteFunction());
        }
        if (type == TINYINT) {
            return WriteMapping.longMapping("Int8", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("Int16", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("Int32", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("Int64", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("Float32", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("Float64", doubleWriteFunction());
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        if (type instanceof CharType || type instanceof VarcharType) {
            return WriteMapping.sliceMapping("String", varcharWriteFunction());
        }
        if (type instanceof VarbinaryType) {
            return WriteMapping.sliceMapping("String", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunction());
        }
        if (type instanceof TimestampType) {
            return WriteMapping.longMapping("DateTime", timestampWriteFunction(TIMESTAMP_MILLIS));
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type);
    }
}
