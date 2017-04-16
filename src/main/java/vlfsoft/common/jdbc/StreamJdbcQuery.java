package vlfsoft.common.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import vlfsoft.common.annotations.design.patterns.gof.StructuralPattern;
import vlfsoft.common.util.ExceptionUtils;

final public class StreamJdbcQuery {
    private StreamJdbcQuery() {
    }

    private static class ResultSetIterator implements Iterator<ResultSetRow> {
        final private ResultSet mResultSet;
        private int mRowIndex;

        private ResultSetRow mResultSetRow;

        public ResultSetIterator(final @Nonnull ResultSet aResultSet) {
            this.mResultSet = aResultSet;
            mRowIndex = 0;
            mResultSetRow = null;
        }

        @Override
        public boolean hasNext() {
            try {
                mResultSetRow = mResultSet.next() ? new ResultSetRow(mResultSet, mRowIndex++) : null;
            } catch (SQLException e) {
                RTSQLException.propagate(e);
            }
            return mResultSetRow != null;

/*
            try {
                // http://stackoverflow.com/questions/867194/java-resultset-how-to-check-if-there-are-any-results
                // Assuming you are working with a newly returned ResultSet whose cursor is pointing before the first row,
                // an easier way to check this is to just call isBeforeFirst().
                // This avoids having to back-track if the data is to be read.
                // As explained in the documentation, this returns false if the cursor is not before the first record or if there are no rows in the ResultSet.

                // PRB: isLast: function not yet implemented for SQLite
                // WO: see. current algorithm with mResultSetRow and wo using mResultSet.isBeforeFirst() || !mResultSet.isLast()

                return mResultSet.isBeforeFirst() || !mResultSet.isLast();
            } catch (SQLException e) {
                RTSQLException.propagate(e);
            }
            return false;
*/
        }

        @Override
        public ResultSetRow next() {
            return mResultSetRow;
/*
            try {
                mHasNext = mResultSet.next();
                return new ResultSetRow(mResultSet, mRowIndex++);
            } catch (SQLException e) {
                RTSQLException.propagate(e);
            }
            return null;
*/
        }


    }

    public static Stream<ResultSetRow> getStreamOf(final @Nonnull ResultSet aResultSet) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ResultSetIterator(aResultSet), Spliterator.IMMUTABLE), false);
    }

    public static class ResultSetRow {
        final private ResultSet mResultSet;
        final private int mRowIndex;

        public ResultSet getResultSet() {
            return mResultSet;
        }

        public int getRowIndex() {
            return mRowIndex;
        }

        public ResultSetRow(final @Nonnull ResultSet aResultSet, int aRowIndex) {
            mResultSet = aResultSet;
            this.mRowIndex = aRowIndex;
        }
    }

    /**
     * Run-time Exception wrapper for SQLException, to avoid problems with overriding methods of Iterator.
     */
    @StructuralPattern.Adapter
    public static class RTSQLException extends RuntimeException {
        private RTSQLException(String aMessage) {
            super(aMessage);
        }

        public static void propagate(SQLException aSQLException) {
            throw new RTSQLException(ExceptionUtils.getStackTrace(aSQLException));
        }

    }
}