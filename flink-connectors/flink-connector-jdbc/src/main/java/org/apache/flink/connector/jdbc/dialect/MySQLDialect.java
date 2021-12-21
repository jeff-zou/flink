/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.MySQLRowConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Optional;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.sql.Types;
import java.util.stream.Collectors;

/** JDBC dialect for MySQL. */
public class MySQLDialect extends AbstractDialect {

    private static final long serialVersionUID = 1L;

    // Define MAX/MIN precision of TIMESTAMP type according to Mysql docs:
    // https://dev.mysql.com/doc/refman/8.0/en/fractional-seconds.html
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 1;

    // Define MAX/MIN precision of DECIMAL type according to Mysql docs:
    // https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
    private static final int MAX_DECIMAL_PRECISION = 65;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:mysql:");
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new MySQLRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long limit) {
        return "LIMIT " + limit;
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.mysql.jdbc.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    /**
     * Mysql upsert query use DUPLICATE KEY UPDATE.
     *
     * <p>NOTE: It requires Mysql's primary key to be consistent with pkFields.
     *
     * <p>We don't use REPLACE INTO, if there are other fields, we can keep their previous values.
     */
    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, int[] fieldTypes, String[] uniqueKeyFields, Boolean cumsum) {
        Boolean[] numericTypes =  Arrays.stream(fieldTypes).mapToObj(i->isNumericInt(i)).toArray(Boolean[]::new);;
        String updateClause = getUpdateClause(fieldNames, numericTypes, uniqueKeyFields, cumsum);
        return getUpsertStatement(tableName, fieldNames, updateClause);
    }

    @Override
    public Optional<String> getUpsertStatement(
        String tableName, String[] fieldNames, LogicalType[] fieldTypes, String[] uniqueKeyFields, Boolean cumsum) {
        Boolean[] numericTypes = Arrays.stream(fieldTypes).map(i->isNumeric(i)).toArray(Boolean[]::new);
        String updateClause = getUpdateClause(fieldNames, numericTypes, uniqueKeyFields, cumsum);
        return getUpsertStatement(tableName, fieldNames, updateClause);
    }

    public String getUpdateClause(String[] fieldNames, Boolean[] numericTypes, String[] uniqueKeyFields, Boolean cumsum) {
        if(cumsum){
            return getCumsumStatement(numericTypes, fieldNames, uniqueKeyFields);
        }else {
            return Arrays.stream(fieldNames)
                .map(f -> quoteIdentifier(f) + "=VALUES(" + quoteIdentifier(f) + ")")
                .collect(Collectors.joining(", "));
        }
    }

    public Optional<String> getUpsertStatement(
        String tableName, String[] fieldNames, String updateClause) {
        return Optional.of(
            getInsertIntoStatement(tableName, fieldNames)
                + " ON DUPLICATE KEY UPDATE "
                + updateClause);
    }

    public String getCumsumStatement(Boolean[] numericTypes, String[] fieldNames, String[] uniqueKeyFields) {
        Set<String> set = new HashSet<String>(Arrays.asList(uniqueKeyFields));
        List<String> res = new ArrayList<String>();

        for(int i=0;i<fieldNames.length;i++){
            if(!set.contains(fieldNames[i]) && numericTypes[i]){
                res.add(quoteIdentifier(fieldNames[i]) + "=VALUES(" + quoteIdentifier(fieldNames[i]) + ") + " + quoteIdentifier(fieldNames[i]));
            }else {
                res.add(quoteIdentifier(fieldNames[i]) + "=VALUES(" + quoteIdentifier(fieldNames[i]) + ")");
            }
        }
        return String.join(", ", res);
    }

    public Boolean isNumericInt(Integer fieldType) {
        if (fieldType == Types.BIGINT || fieldType == Types.INTEGER || fieldType == Types.SMALLINT || fieldType == Types.TINYINT
            || fieldType == Types.DOUBLE || fieldType == Types.FLOAT || fieldType == Types.DECIMAL) {
            return true;
        }
        return false;
    }

    public Boolean isNumeric(LogicalType fieldType) {
        return fieldType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.NUMERIC);
    }

    @Override
    public String dialectName() {
        return "MySQL";
    }

    @Override
    public int maxDecimalPrecision() {
        return MAX_DECIMAL_PRECISION;
    }

    @Override
    public int minDecimalPrecision() {
        return MIN_DECIMAL_PRECISION;
    }

    @Override
    public int maxTimestampPrecision() {
        return MAX_TIMESTAMP_PRECISION;
    }

    @Override
    public int minTimestampPrecision() {
        return MIN_TIMESTAMP_PRECISION;
    }

    @Override
    public List<LogicalTypeRoot> unsupportedTypes() {
        // The data types used in Mysql are list at:
        // https://dev.mysql.com/doc/refman/8.0/en/data-types.html

        // TODO: We can't convert BINARY data type to
        //  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
        // LegacyTypeInfoDataTypeConverter.
        return Arrays.asList(
                LogicalTypeRoot.BINARY,
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE,
                LogicalTypeRoot.INTERVAL_YEAR_MONTH,
                LogicalTypeRoot.INTERVAL_DAY_TIME,
                LogicalTypeRoot.ARRAY,
                LogicalTypeRoot.MULTISET,
                LogicalTypeRoot.MAP,
                LogicalTypeRoot.ROW,
                LogicalTypeRoot.DISTINCT_TYPE,
                LogicalTypeRoot.STRUCTURED_TYPE,
                LogicalTypeRoot.NULL,
                LogicalTypeRoot.RAW,
                LogicalTypeRoot.SYMBOL,
                LogicalTypeRoot.UNRESOLVED);
    }
}
