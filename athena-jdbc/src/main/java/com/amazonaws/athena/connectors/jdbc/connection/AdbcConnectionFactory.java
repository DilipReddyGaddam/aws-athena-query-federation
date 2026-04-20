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
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
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

        // Explicitly load the JDBC driver class so it registers with java.sql.DriverManager.
        // The ADBC JdbcDriver discovers drivers via DriverManager, but unlike HikariCP it does
        // not call Class.forName() itself. Without this, "No suitable driver found" errors occur.
        if (driverClassName != null) {
            try {
                Class.forName(driverClassName);
                LOGGER.info("Loaded JDBC driver class: {}", driverClassName);
            }
            catch (ClassNotFoundException e) {
                LOGGER.error("Failed to load JDBC driver class: {}", driverClassName, e);
                throw new AdbcException("JDBC driver class not found: " + driverClassName,
                        e, AdbcStatusCode.IO, null, 0);
            }
        }

        JdbcDriver driver = new JdbcDriver(allocator);
        AdbcDatabase database = driver.open(driverOptions);
        return database.connect();
    }
}
