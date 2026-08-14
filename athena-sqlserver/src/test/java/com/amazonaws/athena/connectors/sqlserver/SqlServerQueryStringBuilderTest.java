/*-
 * #%L
 * athena-sqlserver
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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
package com.amazonaws.athena.connectors.sqlserver;

import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.QueryPlan;
import com.amazonaws.athena.connectors.jdbc.manager.FederationExpressionParser;
import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.MssqlSqlDialect;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.PARTITION_NUMBER;
import static com.amazonaws.athena.connectors.sqlserver.SqlServerConstants.SQLSERVER_QUOTE_CHARACTER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SqlServerQueryStringBuilderTest
{

    private static final String TEST_PARTITION_COL = "partition_col";
    private static final String TEST_PARTITION_VALUE = "2024";
    private static final String QUOTE_CHAR = "\"";

    static {
        System.setProperty("aws.region", "us-east-1");
    }

    @Test
    public void testQueryBuilder()
    {
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "p0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("p0");
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));
        Assert.assertEquals(" FROM \"default\".\"table\" ", builder.getFromClauseWithSplit("default", "", "table", split));
        Assert.assertEquals(" FROM \"default\".\"schema\".\"table\" ", builder.getFromClauseWithSplit("default", "schema", "table", split));
    }

    @Test
    public void testGetPartitionWhereClauses()
    {
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));

        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperties()).thenReturn(Collections.singletonMap("partition", "0"));
        Mockito.when(split.getProperty(Mockito.eq("partition"))).thenReturn("0");
        Assert.assertEquals(new ArrayList<>(), builder.getPartitionWhereClauses(split));

        Split split1 = Mockito.mock(Split.class);
        Mockito.when(split1.getProperty(SqlServerMetadataHandler.PARTITION_FUNCTION)).thenReturn("pf");
        Mockito.when(split1.getProperty(SqlServerMetadataHandler.PARTITIONING_COLUMN)).thenReturn("col");
        Mockito.when(split1.getProperty(PARTITION_NUMBER)).thenReturn("1");
        Assert.assertEquals(Collections.singletonList(" $PARTITION.pf(col) = 1"), builder.getPartitionWhereClauses(split1));

    }

    @Test
    public void getPartitionWhereClausesWhenPartitionFunctionIsNull()
    {
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(SqlServerMetadataHandler.PARTITION_FUNCTION)).thenReturn(null);
        Mockito.when(split.getProperty(SqlServerMetadataHandler.PARTITIONING_COLUMN)).thenReturn("col");
        Mockito.when(split.getProperty(PARTITION_NUMBER)).thenReturn("74");
        List<String> result = builder.getPartitionWhereClauses(split);
        // Should return empty list if partition function is null.
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void getPartitionWhereClausesWhenPartitioningColumnIsNull()
    {
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(SqlServerMetadataHandler.PARTITION_FUNCTION)).thenReturn("pf");
        Mockito.when(split.getProperty(SqlServerMetadataHandler.PARTITIONING_COLUMN)).thenReturn(null);
        Mockito.when(split.getProperty(PARTITION_NUMBER)).thenReturn("74");
        List<String> result = builder.getPartitionWhereClauses(split);
        // Should return empty list if partitioning column is null.
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void getPartitionWhereClausesWhenPartitionFunctionAndColumnAreNull()
    {
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));
        Split split = Mockito.mock(Split.class);
        Mockito.when(split.getProperty(SqlServerMetadataHandler.PARTITION_FUNCTION)).thenReturn(null);
        Mockito.when(split.getProperty(SqlServerMetadataHandler.PARTITIONING_COLUMN)).thenReturn(null);
        Mockito.when(split.getProperty(PARTITION_NUMBER)).thenReturn("74");
        List<String> result = builder.getPartitionWhereClauses(split);
        // Should return empty list if both partition function and partitioning column are null.
        Assert.assertEquals(0, result.size());
    }

    @org.junit.jupiter.api.Test
    public void testPrepareStatementWithCalciteSql_PartitionClausesBeforeFetchNext_NoExistingWhere()
            throws Exception
    {
        Connection mockConnection = mock(Connection.class);
        FederationExpressionParser expressionParser = mock(FederationExpressionParser.class);
        JdbcSplitQueryBuilder builder = new JdbcSplitQueryBuilder(QUOTE_CHAR, expressionParser)
        {
            @Override
            protected String getFromClauseWithSplit(String catalog, String schema, String table, Split split)
            {
                return " FROM \"" + schema + "\".\"" + table + "\"";
            }

            @Override
            protected List<String> getPartitionWhereClauses(Split split)
            {
                return Collections.emptyList();
            }
        };
        Split split = mock(Split.class);
        when(split.getProperties()).thenReturn(Collections.emptyMap());
        // SELECT * FROM "warehouse"."call_center" ORDER BY "cc_rec_start_date" LIMIT 100
        String base64Plan = "Gl4SXApaGlgKAgoAEk4KTAoCCgASNQoEZ3VpZAoFc2NvcmUKEHBhcnRpdGlvbl9udW1iZXISFAoEYgIQAQoEKgIQAQoEYgIQARgCOg8KA2RibwoIbWxvdXRwdXQYACAK";

        QueryPlan queryPlan = mock(QueryPlan.class);
        Constraints constraintsWithQueryPlan = mock(Constraints.class);
        when(queryPlan.getSubstraitPlan()).thenReturn(base64Plan);
        when(constraintsWithQueryPlan.getQueryPlan()).thenReturn(queryPlan);

        builder.prepareStatementWithCalciteSql(mockConnection, constraintsWithQueryPlan, SqlServerDialect.DEFAULT, split);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockConnection).prepareStatement(sqlCaptor.capture());
        String sql = sqlCaptor.getValue();
        assertEquals("SELECT *\n"
                + "FROM `warehouse`.`call_center` WHERE \"partition_col\" = '2024'\n"
                + "ORDER BY `cc_rec_start_date`\n"
                + "FETCH NEXT 100 ROWS ONLY", sql);
    }

    @Test
    public void testLimitClause()
    {
        Split split = Mockito.mock(Split.class);
        SqlServerQueryStringBuilder builder = new SqlServerQueryStringBuilder(SQLSERVER_QUOTE_CHARACTER, new SqlServerFederationExpressionParser(SQLSERVER_QUOTE_CHARACTER));
        Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), 5L, Collections.emptyMap(), null);
        Assert.assertEquals("", builder.appendLimitOffset(split, constraints));
    }

}
