/*-
 * #%L
 * athena-dynamodb
 * %%
 * Copyright (C) 2023 Amazon Web Services
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
package com.amazonaws.athena.connectors.dynamodb.resolver;

import com.amazonaws.athena.connector.lambda.ThrottlingInvoker;
import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBPaginatedTables;
import com.amazonaws.athena.connectors.dynamodb.model.DynamoDBTable;
import com.amazonaws.athena.connectors.dynamodb.util.DDBTableUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest.UNLIMITED_PAGE_SIZE_VALUE;

/**
 * This class helps with resolving the differences in casing between DynamoDB and Presto. Presto expects all
 * databases, tables, and columns to be lower case. This class allows us to resolve DynamoDB tables
 * which may have captial letters in them without issue. It does so by fetching all table names and doing
 * a case insensitive search over them. It will first try to do a targeted get to reduce the penalty for
 * tables which don't have capitalization.
 *
 * TODO add caching
 */
public class DynamoDBTableResolver
{
    private static final Logger logger = LoggerFactory.getLogger(DynamoDBTableResolver.class);

    private DynamoDbClient ddbClient;
    // used to handle Throttling events using an AIMD strategy for congestion control.
    private ThrottlingInvoker invoker;

    public DynamoDBTableResolver(ThrottlingInvoker invoker, DynamoDbClient ddbClient)
    {
        this.invoker = invoker;
        this.ddbClient = ddbClient;
    }

    /**
     * Fetches the list of tables from DynamoDB via paginated ListTables calls
     *
     * @return the list of tables in DynamoDB
     */
    public DynamoDBPaginatedTables listTables(String token, int pageSize,
                                              AwsRequestOverrideConfiguration awsRequestOverrideConfiguration)
            throws TimeoutException
    {
        return listPaginatedTables(token, pageSize, awsRequestOverrideConfiguration);
    }

    private DynamoDBPaginatedTables listPaginatedTables(String token,
                                                        int pageSize,
                                                        AwsRequestOverrideConfiguration awsRequestOverrideConfiguration) throws TimeoutException
    {
        List<String> tables = new ArrayList<>();
        String nextToken = token;
        int limit = pageSize;
        if (pageSize == UNLIMITED_PAGE_SIZE_VALUE) {
            limit = 100;
        }
        do {
            ListTablesRequest ddbRequest = ListTablesRequest.builder()
                    .exclusiveStartTableName(nextToken)
                    .limit(limit)
                    .overrideConfiguration(awsRequestOverrideConfiguration)
                    .build();
            ListTablesResponse response = invoker.invoke(() -> ddbClient.listTables(ddbRequest));
            tables.addAll(response.tableNames());
            nextToken = response.lastEvaluatedTableName();
        }
        while (nextToken != null && pageSize == UNLIMITED_PAGE_SIZE_VALUE);
        logger.info("{} tables returned with pagination", tables.size());
        return new DynamoDBPaginatedTables(tables, nextToken);
    }

    /**
     * Fetches table schema by first doing a Scan on the given table name, falling back to case insensitive
     * resolution if the table isn't found.  Delegates actual schema derivation to {@link
     * DDBTableUtils#peekTableForSchema}.
     *
     * @param tableName the case insensitive table name
     * @return the table's schema
     */
    public Schema getTableSchema(String tableName, AwsRequestOverrideConfiguration requestOverrideConfiguration)
            throws TimeoutException
    {
        try {
            return DDBTableUtils.peekTableForSchema(tableName, invoker, ddbClient, requestOverrideConfiguration);
        }
        catch (ResourceNotFoundException e) {
            Optional<String> caseInsensitiveMatch = tryCaseInsensitiveSearch(tableName, requestOverrideConfiguration);
            if (caseInsensitiveMatch.isPresent()) {
                return DDBTableUtils.peekTableForSchema(caseInsensitiveMatch.get(), invoker, ddbClient, requestOverrideConfiguration);
            }
            else {
                throw new AthenaConnectorException(e.getMessage(), ErrorDetails.builder().errorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION.toString()).build());
            }
        }
    }

    /**
     * Fetches table metadata by first doing a DescribeTable on the given table table, falling back to case
     * insensitive resolution if the table isn't found.
     *
     * @param tableName the case insensitive table name
     * @return the table's metadata
     */
    public DynamoDBTable getTableMetadata(String tableName,
                                          AwsRequestOverrideConfiguration awsRequestOverrideConfiguration)
            throws TimeoutException
    {
        try {
            return DDBTableUtils.getTable(tableName, invoker, ddbClient, awsRequestOverrideConfiguration);
        }
        catch (ResourceNotFoundException e) {
            Optional<String> caseInsensitiveMatch = tryCaseInsensitiveSearch(tableName, awsRequestOverrideConfiguration);
            if (caseInsensitiveMatch.isPresent()) {
                return DDBTableUtils.getTable(caseInsensitiveMatch.get(), invoker, ddbClient, awsRequestOverrideConfiguration);
            }
            else {
                throw e;
            }
        }
    }

    /*
    Performs a case insensitive table search by listing the tables, mapping them to their lowercase transformation,
    and then mapping the given tableName back to a unique table. To prevent ambiguity, an IllegalStateException is
    thrown if multiple tables map to the given tableName.
     */
    private Optional<String> tryCaseInsensitiveSearch(String tableName, AwsRequestOverrideConfiguration awsRequestOverrideConfiguration)
            throws TimeoutException
    {
        logger.info("Table {} not found.  Falling back to case insensitive search.", tableName);
        Multimap<String, String> lowerCaseNameMapping = ArrayListMultimap.create();
        List<String> tableNames = listPaginatedTables(null, UNLIMITED_PAGE_SIZE_VALUE, awsRequestOverrideConfiguration).getTables();
        for (String nextTableName : tableNames) {
            lowerCaseNameMapping.put(nextTableName.toLowerCase(Locale.ENGLISH), nextTableName);
        }
        Collection<String> mappedNames = lowerCaseNameMapping.get(tableName);
        if (mappedNames.size() > 1) {
            throw new IllegalStateException(String.format("Multiple tables resolved from case insensitive name %s: %s", tableName, mappedNames));
        }
        else if (mappedNames.size() == 1) {
            return Optional.of(mappedNames.iterator().next());
        }
        else {
            return Optional.empty();
        }
    }
}
