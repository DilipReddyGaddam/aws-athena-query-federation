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

import com.amazonaws.athena.connectors.jdbc.manager.JdbcSplitQueryBuilder;
import com.amazonaws.athena.connectors.jdbc.manager.TypeAndValue;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PostGreSqlAdbcRecordHandlerTest
{
    @Test
    public void testInlineParametersWithNoParams()
    {
        String sql = "SELECT * FROM test_table";
        String result = PostGreSqlAdbcRecordHandler.inlineParameters(sql, Collections.emptyList());
        Assert.assertEquals(sql, result);
    }

    @Test
    public void testInlineParametersWithNullParams()
    {
        String sql = "SELECT * FROM test_table";
        String result = PostGreSqlAdbcRecordHandler.inlineParameters(sql, null);
        Assert.assertEquals(sql, result);
    }

    @Test
    public void testInlineParametersWithIntegerParam()
    {
        String sql = "SELECT * FROM test_table WHERE id = ?";
        List<TypeAndValue> params = Collections.singletonList(
                new TypeAndValue(Types.MinorType.INT.getType(), 42));
        String result = PostGreSqlAdbcRecordHandler.inlineParameters(sql, params);
        Assert.assertEquals("SELECT * FROM test_table WHERE id = 42", result);
    }

    @Test
    public void testInlineParametersWithStringParam()
    {
        String sql = "SELECT * FROM test_table WHERE name = ?";
        List<TypeAndValue> params = Collections.singletonList(
                new TypeAndValue(Types.MinorType.VARCHAR.getType(), "test_value"));
        String result = PostGreSqlAdbcRecordHandler.inlineParameters(sql, params);
        Assert.assertEquals("SELECT * FROM test_table WHERE name = 'test_value'", result);
    }

    @Test
    public void testInlineParametersWithStringEscaping()
    {
        String sql = "SELECT * FROM test_table WHERE name = ?";
        List<TypeAndValue> params = Collections.singletonList(
                new TypeAndValue(Types.MinorType.VARCHAR.getType(), "O'Brien"));
        String result = PostGreSqlAdbcRecordHandler.inlineParameters(sql, params);
        Assert.assertEquals("SELECT * FROM test_table WHERE name = 'O''Brien'", result);
    }

    @Test
    public void testInlineParametersWithMultipleParams()
    {
        String sql = "SELECT * FROM test_table WHERE id > ? AND name = ? AND score < ?";
        List<TypeAndValue> params = Arrays.asList(
                new TypeAndValue(Types.MinorType.BIGINT.getType(), 100L),
                new TypeAndValue(Types.MinorType.VARCHAR.getType(), "test"),
                new TypeAndValue(Types.MinorType.FLOAT8.getType(), 99.5));
        String result = PostGreSqlAdbcRecordHandler.inlineParameters(sql, params);
        Assert.assertEquals("SELECT * FROM test_table WHERE id > 100 AND name = 'test' AND score < 99.5", result);
    }

    @Test
    public void testInlineParametersWithBooleanParam()
    {
        String sql = "SELECT * FROM test_table WHERE active = ?";
        List<TypeAndValue> params = Collections.singletonList(
                new TypeAndValue(Types.MinorType.BIT.getType(), true));
        String result = PostGreSqlAdbcRecordHandler.inlineParameters(sql, params);
        Assert.assertEquals("SELECT * FROM test_table WHERE active = true", result);
    }

    @Test
    public void testInlineParametersWithDecimalParam()
    {
        String sql = "SELECT * FROM test_table WHERE amount = ?";
        List<TypeAndValue> params = Collections.singletonList(
                new TypeAndValue(new ArrowType.Decimal(10, 2, 128), new BigDecimal("1234.56")));
        String result = PostGreSqlAdbcRecordHandler.inlineParameters(sql, params);
        Assert.assertEquals("SELECT * FROM test_table WHERE amount = 1234.56", result);
    }
}
