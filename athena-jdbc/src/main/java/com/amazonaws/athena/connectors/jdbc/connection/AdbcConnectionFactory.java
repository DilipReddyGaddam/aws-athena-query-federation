/*-
 * #%L
 * athena-jdbc
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
package com.amazonaws.athena.connectors.jdbc.connection;

import com.amazonaws.athena.connector.credentials.CredentialsProvider;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;

/**
 * Creates ADBC connections by wrapping an existing JDBC driver via adbc-driver-jdbc.
 * This allows databases to return data natively as Arrow columnar batches.
 */
public class AdbcConnectionFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AdbcConnectionFactory.class);

    private final DatabaseConnectionConfig databaseConnectionConfig;
    private final Map<String, String> jdbcProperties;
    private final String driverClassName;

    public AdbcConnectionFactory(final DatabaseConnectionConfig databaseConnectionConfig,
            final Map<String, String> jdbcProperties)
    {
        this(databaseConnectionConfig, jdbcProperties, null);
    }

    public AdbcConnectionFactory(final DatabaseConnectionConfig databaseConnectionConfig,
            final Map<String, String> jdbcProperties,
            final DatabaseConnectionInfo databaseConnectionInfo)
    {
        this.databaseConnectionConfig = Validate.notNull(databaseConnectionConfig, "databaseConnectionConfig must not be null");
        this.jdbcProperties = jdbcProperties != null ? new HashMap<>(jdbcProperties) : new HashMap<>();
        this.driverClassName = databaseConnectionInfo != null ? databaseConnectionInfo.getDriverClassName() : null;
    }

    /**
     * Creates an ADBC connection wrapping the configured JDBC driver.
     *
     * @param credentialsProvider provides database credentials (username/password from Secrets Manager)
     * @param allocator Arrow buffer allocator for ADBC operations
     * @return an AdbcConnection that returns query results as Arrow batches
     * @throws AdbcException if connection fails
     */
    public AdbcConnection getConnection(final CredentialsProvider credentialsProvider,
            final BufferAllocator allocator) throws AdbcException
    {
        final String derivedJdbcString;
        Map<String, Object> driverOptions = new HashMap<>();

        if (credentialsProvider != null) {
            Matcher secretMatcher = GenericJdbcConnectionFactory.SECRET_NAME_PATTERN
                    .matcher(databaseConnectionConfig.getJdbcConnectionString());
            String jdbcUrl = secretMatcher.replaceAll(Matcher.quoteReplacement(""));
            // Remove trailing '?' left after secret removal (e.g., "jdbc:postgresql://host/db?" -> "jdbc:postgresql://host/db")
            derivedJdbcString = jdbcUrl.endsWith("?") ? jdbcUrl.substring(0, jdbcUrl.length() - 1) : jdbcUrl;

            Map<String, String> credentials = credentialsProvider.getCredentialMap();
            if (credentials.containsKey("user")) {
                driverOptions.put(AdbcDriver.PARAM_USERNAME.getKey(), credentials.get("user"));
            }
            if (credentials.containsKey("password")) {
                driverOptions.put(AdbcDriver.PARAM_PASSWORD.getKey(), credentials.get("password"));
            }
        }
        else {
            derivedJdbcString = databaseConnectionConfig.getJdbcConnectionString();
        }

        driverOptions.put(AdbcDriver.PARAM_URI.getKey(), derivedJdbcString);

        // Pass additional JDBC properties
        for (Map.Entry<String, String> entry : jdbcProperties.entrySet()) {
            driverOptions.put(entry.getKey(), entry.getValue());
        }

        LOGGER.info("Creating ADBC connection to: {}", derivedJdbcString);

        loadDriverClass();

        JdbcDriver driver = new JdbcDriver(allocator);
        AdbcDatabase database = driver.open(driverOptions);
        return database.connect();
    }

    /**
     * Creates a native ADBC connection via JNI to the native PostgreSQL driver.
     * Requires the native .so to be available (e.g., via Lambda layer at /opt/lib/).
     */
    public AdbcConnection getNativeConnection(final CredentialsProvider credentialsProvider,
            final BufferAllocator allocator, final String nativeDriverPath) throws AdbcException
    {
        final String derivedJdbcString;
        if (credentialsProvider != null) {
            Matcher secretMatcher = GenericJdbcConnectionFactory.SECRET_NAME_PATTERN
                    .matcher(databaseConnectionConfig.getJdbcConnectionString());
            String jdbcUrl = secretMatcher.replaceAll(Matcher.quoteReplacement(""));
            derivedJdbcString = jdbcUrl.endsWith("?") ? jdbcUrl.substring(0, jdbcUrl.length() - 1) : jdbcUrl;
        }
        else {
            derivedJdbcString = databaseConnectionConfig.getJdbcConnectionString();
        }

        String pgUri = derivedJdbcString.replace("jdbc:postgresql://", "postgresql://");
        if (credentialsProvider != null) {
            Map<String, String> creds = credentialsProvider.getCredentialMap();
            String user = creds.getOrDefault("user", "");
            String password = creds.getOrDefault("password", "");
            pgUri = pgUri.replace("postgresql://", "postgresql://" + user + ":" + password + "@");
        }

        Map<String, Object> params = new HashMap<>();
        params.put(JniDriver.PARAM_DRIVER.getKey(), nativeDriverPath);
        params.put("uri", pgUri);

        LOGGER.info("Creating native ADBC connection via JNI");

        JniDriver driver = new JniDriver(allocator);
        AdbcDatabase database = driver.open(params);
        return database.connect();
    }

    /**
     * Creates a raw JDBC connection for use with the Arrow JDBC adapter directly.
     * This bypasses the ADBC layer, allowing custom JdbcToArrowConfig.
     */
    public Connection getJdbcConnection(final CredentialsProvider credentialsProvider) throws SQLException
    {
        final String derivedJdbcString;
        Properties connectionProps = new Properties();
        connectionProps.putAll(jdbcProperties);

        if (credentialsProvider != null) {
            Matcher secretMatcher = GenericJdbcConnectionFactory.SECRET_NAME_PATTERN
                    .matcher(databaseConnectionConfig.getJdbcConnectionString());
            String jdbcUrl = secretMatcher.replaceAll(Matcher.quoteReplacement(""));
            derivedJdbcString = jdbcUrl.endsWith("?") ? jdbcUrl.substring(0, jdbcUrl.length() - 1) : jdbcUrl;
            connectionProps.putAll(credentialsProvider.getCredentialMap());
        }
        else {
            derivedJdbcString = databaseConnectionConfig.getJdbcConnectionString();
        }

        loadDriverClass();
        LOGGER.info("Creating direct JDBC connection to: {}", derivedJdbcString);
        return DriverManager.getConnection(derivedJdbcString, connectionProps);
    }

    private void loadDriverClass()
    {
        if (driverClassName != null) {
            try {
                Class.forName(driverClassName);
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException("JDBC driver class not found: " + driverClassName, e);
            }
        }
    }
}
