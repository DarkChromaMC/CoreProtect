package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/**
 * The loaded index is read from the database once and then kept up to date by the consumer's batches. Reading it again
 * after every completed segment cost a large server more native memory than it had to spare.
 */
class DuckDBSpatialIndexCacheTest {

    private static final long SEGMENT = DuckDBSpatialIndex.SEGMENT_ROWS;

    @TempDir
    Path directory;

    private Connection connection;
    private DatabaseType previousType;
    private String previousPrefix;

    @BeforeEach
    void open() throws Exception {
        previousType = ConfigHandler.databaseType;
        previousPrefix = ConfigHandler.prefix;
        ConfigHandler.databaseType = DatabaseType.DUCKDB;
        ConfigHandler.prefix = "co_";
        connection = DriverManager.getConnection("jdbc:duckdb:" + directory.resolve("test.duckdb"));
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE co_block (rowid BIGINT NOT NULL, time INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER)");
            for (String table : new String[] { "container", "item", "sign", "chat", "command", "session" }) {
                statement.execute("CREATE TABLE co_" + table + " (rowid INTEGER NOT NULL, time INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER)");
            }
            for (String table : new String[] { "entity_container", "entity_interaction" }) {
                statement.execute("CREATE TABLE co_" + table + " (rowid INTEGER NOT NULL, time INTEGER, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, entity_spawn_rowid INTEGER)");
            }
            DuckDBSpatialIndex.createTable("co_", statement);
            // two segments far from the origin, plus one row at chunk 0,0 in the first
            statement.execute("INSERT INTO co_block SELECT i, i, 1, CASE WHEN i = 5 THEN 8 ELSE 4000 + (i % 3000) END, 64, CASE WHEN i = 5 THEN 8 ELSE 4000 + (i % 3000) END FROM range(1, " + (2 * SEGMENT + 1) + ") t(i)");
        }
        DuckDBSpatialIndex.Builder builder = DuckDBSpatialIndex.builder("co_");
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("SELECT rowid, wid, x, z FROM co_block ORDER BY rowid")) {
            while (resultSet.next()) {
                builder.add("block", resultSet.getLong(1), resultSet.getInt(2), resultSet.getInt(3), resultSet.getInt(4));
            }
        }
        builder.writeCompleted(connection);
        // the consumer's first batch after a start rebuilds its writer state and drops the loaded index once; do that here
        DuckDBSpatialIndex.begin(connection, "co_");
        DuckDBSpatialIndex.cacheLoads = 0;
    }

    @AfterEach
    void close() throws Exception {
        DuckDBSpatialIndex.afterCacheLoad = null;
        DuckDBSpatialIndex.reset();
        connection.close();
        ConfigHandler.databaseType = previousType;
        ConfigHandler.prefix = previousPrefix;
    }

    /** The row ranges a lookup of this block position reads, as "start-end" (the open tail ends at Long.MAX_VALUE). */
    private String ranges(int x, int z) {
        List<DuckDBSpatialIndex.RowRange> ranges = DuckDBSpatialIndex.spatialRanges(connection, "co_", "block", 1, x, x, z, z, null, null);
        assertNotNull(ranges, "the index should narrow this lookup");
        String expression = DuckDBSpatialIndex.tableExpression(connection, "co_", "block", 1, x, x, z, z);
        return expression.replaceAll(".*?WHERE (rowid[^U)]*)(?: UNION ALL |\\).*)", "$1;").trim();
    }

    /** A consumer batch that logs one whole segment of block rows at this position and commits. */
    private void writeSegment(long firstRowId, int x, int z) throws Exception {
        DuckDBSpatialIndex.Transaction transaction = DuckDBSpatialIndex.begin(connection, "co_");
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO co_block SELECT i, i, 1, " + x + ", 64, " + z + " FROM range(" + firstRowId + ", " + (firstRowId + SEGMENT) + ") t(i)");
        }
        for (long rowId = firstRowId; rowId < firstRowId + SEGMENT; rowId++) {
            transaction.addBlock(rowId, 1, x, z);
        }
        transaction.flush(connection);
        transaction.publish();
    }

    @Test
    void lookupsReadTheIndexOnce() {
        String first = ranges(8, 8);
        ranges(8, 8);
        // a position every segment holds can't be narrowed, but still uses the loaded index
        DuckDBSpatialIndex.spatialRanges(connection, "co_", "block", 1, 4500, 4500, 4500, 4500, null, null);
        assertEquals(1, DuckDBSpatialIndex.cacheLoads);
        assertEquals("rowid BETWEEN 1 AND " + SEGMENT + ";rowid > " + (2 * SEGMENT) + ";", first);
    }

    @Test
    void aCompletedSegmentIsAddedWithoutReadingTheIndexAgain() throws Exception {
        ranges(8, 8);
        writeSegment(2 * SEGMENT + 1, -9000, -9000);

        // the new segment is known: a lookup elsewhere skips it, a lookup there reads it
        assertEquals("rowid BETWEEN 1 AND " + SEGMENT + ";rowid > " + (3 * SEGMENT) + ";", ranges(8, 8));
        assertEquals("rowid > " + (2 * SEGMENT) + ";", ranges(-9000, -9000));
        assertEquals(1, DuckDBSpatialIndex.cacheLoads);

        // and it matches what a fresh read of the table gives
        DuckDBSpatialIndex.reset();
        assertEquals("rowid BETWEEN 1 AND " + SEGMENT + ";rowid > " + (3 * SEGMENT) + ";", ranges(8, 8));
        assertEquals(2, DuckDBSpatialIndex.cacheLoads);
    }

    @Test
    void aSegmentCompletedDuringTheReadIsNotLostAndCausesNoSecondRead() throws Exception {
        boolean[] ran = { false };
        DuckDBSpatialIndex.afterCacheLoad = () -> {
            if (ran[0]) {
                return;
            }
            ran[0] = true;
            try {
                writeSegment(2 * SEGMENT + 1, -9000, -9000);
            }
            catch (Exception exception) {
                throw new IllegalStateException(exception);
            }
        };

        assertEquals("rowid BETWEEN 1 AND " + SEGMENT + ";rowid > " + (3 * SEGMENT) + ";", ranges(8, 8));
        assertTrue(ran[0]);
        assertEquals(1, DuckDBSpatialIndex.cacheLoads);
    }

    @Test
    void aSegmentCommittedBeforeTheReadButPublishedAfterIsNotAddedTwice() throws Exception {
        DuckDBSpatialIndex.Transaction transaction = DuckDBSpatialIndex.begin(connection, "co_");
        for (long rowId = 2 * SEGMENT + 1; rowId <= 3 * SEGMENT; rowId++) {
            transaction.addBlock(rowId, 1, -9000, -9000);
        }
        transaction.flush(connection);
        // committed (autocommit), not yet published: the read already sees the segment
        assertEquals("rowid BETWEEN 1 AND " + SEGMENT + ";rowid > " + (3 * SEGMENT) + ";", ranges(8, 8));
        transaction.publish();
        assertEquals("rowid BETWEEN 1 AND " + SEGMENT + ";rowid > " + (3 * SEGMENT) + ";", ranges(8, 8));
        assertEquals("rowid > " + (2 * SEGMENT) + ";", ranges(-9000, -9000));
        assertEquals(1, DuckDBSpatialIndex.cacheLoads);
    }

    @Test
    void aResetDuringTheReadMakesTheLookupReadAgain() {
        boolean[] ran = { false };
        DuckDBSpatialIndex.afterCacheLoad = () -> {
            if (!ran[0]) {
                ran[0] = true;
                DuckDBSpatialIndex.reset();
            }
        };
        ranges(8, 8);
        assertEquals(2, DuckDBSpatialIndex.cacheLoads);
    }
}
