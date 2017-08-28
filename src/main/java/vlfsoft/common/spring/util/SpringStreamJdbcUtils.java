package vlfsoft.common.spring.util;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import vlfsoft.common.jdbc.StreamJdbcQuery;

final public class SpringStreamJdbcUtils {

    private SpringStreamJdbcUtils() {
    }

    /**
     * works with {@link ResultSet} as with Stream
     */
    public static class StreamResultSetExtractor implements ResultSetExtractor<Void> {

        final @Nonnull StreamResultSetRowExtractor mStreamResultSetRowExtractor;

        // PROBLEM: JdbcTemplate calls extractData from StreamResultSetExtractor<RowType>, extractData returns Stream<RowType>,
        // JdbcTemplate calls JdbcUtils.closeResultSet(rs); and thus ResulSet is closed before Stream calls method hasNext, next.
        // As a result: "java.sql.SQLException: Operation not allowed after ResultSet closed".
        // With RxJava there is no this problem because of see. "RxJava vs Streams" in Java development.docx
        // In more details: Stream starts to process elements of stream after extractData finished, when first stream method is called
        // In RptRepositoryTest this method is collect.
        // RxJava Observable emits event for each row in ResultSet inside extractData:
        // See. ObservableResultSetExtractor.onNext and RxJdbcQuery.onNext
        // Solution: Wait until Stream is implemented directly in the JdbcTemplate.
        @Override
        public Void extractData(ResultSet aResultSet) throws DataAccessException {
            mStreamResultSetRowExtractor.extractData(StreamJdbcQuery.getStreamOf(aResultSet));
            return null;
        }

        public StreamResultSetExtractor(final @Nonnull StreamResultSetRowExtractor aStreamResultSetRowExtractor) {
            mStreamResultSetRowExtractor = aStreamResultSetRowExtractor;
        }

    }

    public interface StreamResultSetRowExtractor {
        void extractData(Stream<StreamJdbcQuery.ResultSetRow> aStreamResultSetRow);
    }

    /**
     * See <a href="http://www.java2s.com/Tutorials/Java/Data_Type_How_to/Date_Convert/Convert_java_sql_Timestamp_to_LocalDateTime.htm">Java Data Type How to - Convert java.sql.Timestamp to LocalDateTime</a>
     * To avoid dependencies doubled this method from vlfsoft.common.nui.rxjdbc.RxJdbcQuery
     */
    public static Optional<LocalDateTime> getLocalDateTime(final ResultSet aResultSet, int i) throws SQLException {
        Timestamp timestamp = aResultSet.getTimestamp(i);
        return !aResultSet.wasNull() && timestamp != null ? Optional.of(LocalDateTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault())) : Optional.empty();
    }

    /**
     * See <a href="http://stackoverflow.com/questions/8992282/convert-localdate-to-localdatetime-or-java-sql-timestamp">Convert LocalDate to LocalDateTime or java.sql.Timestamp</a>
     * To avoid dependencies doubled this method from vlfsoft.common.nui.rxjdbc.RxJdbcQuery
     */
    public static Timestamp getTimestamp(Optional<LocalDateTime> aLocalDateTime) throws SQLException {
        return aLocalDateTime.map(Timestamp::valueOf).orElse(null);
    }

    public static Timestamp getTimestamp(LocalDateTime aLocalDateTime) throws SQLException {
        return Timestamp.valueOf(aLocalDateTime);
    }

    /**
     * To avoid dependencies doubled this method from vlfsoft.common.spring.util.SpringRxJdbcUtils
     */
    public static <T> Optional<T> getOptionalColumnValue(T aColumnValue, ResultSet aResultSet) throws SQLException {
        return aResultSet.wasNull() ? Optional.empty() : Optional.of(aColumnValue);
    }

}
