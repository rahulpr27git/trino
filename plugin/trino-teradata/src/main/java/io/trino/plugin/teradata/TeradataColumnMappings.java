package io.trino.plugin.teradata;
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
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.spi.TrinoException;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

public interface TeradataColumnMappings
{
    static LongReadFunction dateReadFunctionUsingLocalDate()
    {
        return new LongReadFunction() {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                // 'ResultSet.getObject' with class name may throwing an exception for teradata
                // e.g. in Teradata driver, rs.getObject(int, Class) throws exception of missing implementation
                resultSet.getObject(columnIndex);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                Date value = (Date) resultSet.getObject(columnIndex);
                if (value == null) {
                    throw new TrinoException(JDBC_ERROR, "Driver returned null Date for a non-null value");
                }

                return value.toLocalDate().toEpochDay();
            }
        };
    }
}
