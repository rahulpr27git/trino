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
package io.trino.plugin.teradata;

import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunctionUsingLocalDate;
import static io.trino.plugin.jdbc.StandardColumnMappings.longTimestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toLongTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static java.util.Objects.requireNonNull;

public final class TeradataColumnMappings
{
    private static final int MAX_LOCAL_DATE_TIME_PRECISION = 9;

    private TeradataColumnMappings() {}

    public static ColumnMapping dateColumnMappingUsingLocalDate()
    {
        return ColumnMapping.longMapping(
                DATE,
                dateReadFunctionUsingLocalDate(),
                dateWriteFunctionUsingLocalDate());
    }

    public static LongReadFunction dateReadFunctionUsingLocalDate()
    {
        return new LongReadFunction() {
            @Override
            public boolean isNull(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                resultSet.getObject(columnIndex);
                return resultSet.wasNull();
            }

            @Override
            public long readLong(ResultSet resultSet, int columnIndex)
                    throws SQLException
            {
                final Date date = resultSet.getDate(columnIndex);
                LocalDate value = date.toLocalDate();
                // Some drivers (e.g. MemSQL's) return null LocalDate even though the value isn't null
                if (value == null) {
                    throw new TrinoException(JDBC_ERROR, "Driver returned null LocalDate for a non-null value");
                }

                return value.toEpochDay();
            }
        };
    }

    public static ColumnMapping timeColumnMapping(TimeType timeType)
    {
        return ColumnMapping.longMapping(
                timeType,
                timeReadFunction(timeType),
                timeWriteFunction(timeType.getPrecision()));
    }

    public static LongReadFunction timeReadFunction(TimeType timeType)
    {
        requireNonNull(timeType, "timeType is null");
        checkArgument(timeType.getPrecision() <= 9, "Unsupported type precision: %s", timeType);
        return (resultSet, columnIndex) -> {
            final Time time = resultSet.getTime(columnIndex);
            long nanosOfDay = time.toLocalTime().toNanoOfDay();
            verify(nanosOfDay < NANOSECONDS_PER_DAY, "Invalid value of nanosOfDay: %s", nanosOfDay);
            long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
            long rounded = round(picosOfDay, 12 - timeType.getPrecision());
            if (rounded == PICOSECONDS_PER_DAY) {
                rounded = 0;
            }
            return rounded;
        };
    }

    public static ColumnMapping timestampColumnMapping(TimestampType timestampType)
    {
        if (timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    timestampType,
                    timestampReadFunction(timestampType),
                    timestampWriteFunction(timestampType));
        }
        checkArgument(timestampType.getPrecision() <= MAX_LOCAL_DATE_TIME_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ColumnMapping.objectMapping(
                timestampType,
                longTimestampReadFunction(timestampType),
                longTimestampWriteFunction(timestampType, timestampType.getPrecision()));
    }

    public static LongReadFunction timestampReadFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return (resultSet, columnIndex) -> {
            final Timestamp timestamp = resultSet.getTimestamp(columnIndex);
            return toTrinoTimestamp(timestampType, timestamp.toLocalDateTime());
        };
    }

    private static ObjectReadFunction longTimestampReadFunction(TimestampType timestampType)
    {
        checkArgument(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION && timestampType.getPrecision() <= MAX_LOCAL_DATE_TIME_PRECISION,
                "Precision is out of range: %s", timestampType.getPrecision());
        return ObjectReadFunction.of(
                LongTimestamp.class,
                (resultSet, columnIndex) -> toLongTrinoTimestamp(timestampType, resultSet.getObject(columnIndex, LocalDateTime.class)));
    }
}
