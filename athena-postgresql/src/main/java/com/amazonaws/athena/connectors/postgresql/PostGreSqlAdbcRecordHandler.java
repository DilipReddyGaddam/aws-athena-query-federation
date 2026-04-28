/*-
 * #%L
 * athena-postgresql
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.substrait.SubstraitSqlUtils;
import com.amazonaws.athena.connectors.jdbc.connection.AdbcConnectionFactory;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionConfig;
import com.amazonaws.athena.connectors.jdbc.connection.DatabaseConnectionInfo;
import com.amazonaws.athena.connectors.jdbc.manager.AdbcRecordHandler;
import com.amazonaws.athena.connectors.jdbc.manager.JDBCUtil;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import com.amazonaws.athena.connectors.jdbc.qpt.JdbcQueryPassthrough;
import com.google.common.annotations.VisibleForTesting;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DEFAULT_PORT;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRESQL_DRIVER_CLASS;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_NAME;
import static com.amazonaws.athena.connectors.postgresql.PostGreSqlConstants.POSTGRES_QUOTE_CHARACTER;

/**
 * PostgreSQL record handler using the Arrow JDBC adapter directly for native Arrow columnar
 * data retrieval with custom JdbcToArrowConfig type mappings.
 */
public class PostGreSqlAdbcRecordHandler
        extends AdbcRecordHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PostGreSqlAdbcRecordHandler.class);

    private final JdbcSplitQueryBuilder jdbcSplitQueryBuilder;
    private final JdbcQueryPassthrough queryPassthrough = new JdbcQueryPassthrough();

    public PostGreSqlAdbcRecordHandler(Map<String, String> configOptions)
    {
        this(JDBCUtil.getSingleDatabaseConfigFromEnv(POSTGRES_NAME, configOptions), configOptions);
    }

    public PostGreSqlAdbcRecordHandler(DatabaseConnectionConfig databaseConnectionConfig,
            Map<String, String> configOptions)
    {
        this(databaseConnectionConfig,
                S3Client.create(),
                SecretsManagerClient.create(),
                AthenaClient.create(),
                new AdbcConnectionFactory(databaseConnectionConfig, PostGreSqlMetadataHandler.JDBC_PROPERTIES,
                        new DatabaseConnectionInfo(POSTGRESQL_DRIVER_CLASS, POSTGRESQL_DEFAULT_PORT)),
                new PostGreSqlQueryStringBuilder(POSTGRES_QUOTE_CHARACTER,
                        new PostgreSqlFederationExpressionParser(POSTGRES_QUOTE_CHARACTER)),
                configOptions);
    }

    @VisibleForTesting
    protected PostGreSqlAdbcRecordHandler(DatabaseConnectionConfig databaseConnectionConfig,
            S3Client amazonS3, SecretsManagerClient secretsManager, AthenaClient athena,
            AdbcConnectionFactory adbcConnectionFactory, JdbcSplitQueryBuilder jdbcSplitQueryBuilder,
            Map<String, String> configOptions)
    {
        super(amazonS3, secretsManager, athena, databaseConnectionConfig, adbcConnectionFactory, configOptions);
        this.jdbcSplitQueryBuilder = Validate.notNull(jdbcSplitQueryBuilder, "query builder must not be null");
    }

    private static final String NATIVE_DRIVER_PATH = "/opt/lib/libadbc_driver_postgresql.so";

    @Override
    protected String getNativeDriverPath()
    {
        if (new java.io.File(NATIVE_DRIVER_PATH).exists()) {
            LOGGER.info("Native ADBC driver found at {}", NATIVE_DRIVER_PATH);
            return NATIVE_DRIVER_PATH;
        }
        LOGGER.info("Native ADBC driver not found, using Arrow JDBC adapter");
        return null;
    }

    @Override
    protected String buildSqlForAdbc(ReadRecordsRequest request)
    {
        if (request.getConstraints().isQueryPassThrough()) {
            queryPassthrough.verify(request.getConstraints().getQueryPassthroughArguments());
            return request.getConstraints().getQueryPassthroughArguments().get(JdbcQueryPassthrough.QUERY);
        }

        if (request.getConstraints().getQueryPlan() != null
                && request.getConstraints().getQueryPlan().getSubstraitPlan() != null) {
            String plan = request.getConstraints().getQueryPlan().getSubstraitPlan();
            SqlNode sqlNode = SubstraitSqlUtils.getSqlNodeFromSubstraitPlan(plan, PostgresqlSqlDialect.DEFAULT);
            String sql = sqlNode.toSqlString(PostgresqlSqlDialect.DEFAULT).getSql();

            // Append split range clause if present
            String partitionSchemaName = request.getSplit().getProperty("partition_schema_name");
            String partitionName = request.getSplit().getProperty("partition_name");
            if ("*".equals(partitionSchemaName) && partitionName != null && !"*".equals(partitionName)) {
                sql = sql.toUpperCase().contains("WHERE")
                        ? sql + " AND " + partitionName
                        : sql + " WHERE " + partitionName;
            }

            LOGGER.info("QueryPlan SQL: {}", sql);
            return sql;
        }

        JdbcSplitQueryBuilder.SqlComponents components = jdbcSplitQueryBuilder.buildSqlComponents(
                null,
                request.getTableName().getSchemaName(),
                request.getTableName().getTableName(),
                request.getSchema(),
                request.getConstraints(),
                request.getSplit());

        return inlineParameters(components.getSql(), components.getParameters());
    }

    /**
     * Replaces '?' placeholders in the SQL string with actual parameter values.
     * This is a POC approach; production code should use prepared statement parameter binding.
     */
    static String inlineParameters(String sql, List<TypeAndValue> params)
    {
        if (params == null || params.isEmpty()) {
            return sql;
        }

        StringBuilder result = new StringBuilder();
        int paramIndex = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '?' && paramIndex < params.size()) {
                result.append(formatValue(params.get(paramIndex)));
                paramIndex++;
            }
            else {
                result.append(c);
            }
        }
        return result.toString();
    }

    private static String formatValue(TypeAndValue typeAndValue)
    {
        ArrowType type = typeAndValue.getType();
        Object value = typeAndValue.getValue();
        Types.MinorType minorType = Types.getMinorTypeForArrowType(type);

        switch (minorType) {
            case BIGINT:
            case INT:
            case SMALLINT:
            case TINYINT:
            case FLOAT4:
            case FLOAT8:
                return String.valueOf(value);
            case BIT:
                return Boolean.toString((boolean) value);
            case DECIMAL:
                return ((BigDecimal) value).toPlainString();
            case VARCHAR:
                return "'" + String.valueOf(value).replace("'", "''") + "'";
            case VARBINARY:
                byte[] bytes = (byte[]) value;
                StringBuilder hex = new StringBuilder("E'\\\\x");
                for (byte b : bytes) {
                    hex.append(String.format("%02x", b));
                }
                hex.append("'");
                return hex.toString();
            case DATEDAY:
                long utcMillis = TimeUnit.DAYS.toMillis(((Number) value).longValue());
                TimeZone aDefault = TimeZone.getDefault();
                int offset = aDefault.getOffset(utcMillis);
                utcMillis -= offset;
                return "'" + new Date(utcMillis).toString() + "'";
            case DATEMILLI:
                LocalDateTime timestamp = (LocalDateTime) value;
                return "'" + new Timestamp(timestamp.toInstant(ZoneOffset.UTC).toEpochMilli()).toString() + "'";
            default:
                LOGGER.warn("Unhandled type {} for parameter inlining, using toString()", minorType);
                return "'" + String.valueOf(value).replace("'", "''") + "'";
        }
    }
}
