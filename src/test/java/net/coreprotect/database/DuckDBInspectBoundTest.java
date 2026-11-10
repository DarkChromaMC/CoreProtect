package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Inspector lookups stay inside the spatial index or are refused, instead of scanning the whole table. */
class DuckDBInspectBoundTest {

    private static final String PREFIX = "co_";
    private static final long SEGMENT = DuckDBSpatialIndex.SEGMENT_ROWS;

    @TempDir
    Path directory;

    private Connection connection;

    @BeforeEach
    void open() throws Exception {
        connection = DriverManager.getConnection("jdbc:duckdb:" + directory.resolve("test.duckdb"));
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE co_block (rowid BIGINT NOT NULL, wid INTEGER, x INTEGER, y INTEGER, z INTEGER, action TINYINT)");
            DuckDBSpatialIndex.createTable(PREFIX, statement);
        }
    }

    @AfterEach
    void close() throws Exception {
        DuckDBSpatialIndex.reset();
        connection.close();
    }

    /** Three full segments: a "quiet" block only in the first, a "busy" block in every one. */
    private void writeIndexedRows(long extraRows) throws Exception {
        long rows = 3 * SEGMENT;
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO co_block SELECT i, 1, CASE WHEN i % 1000 = 0 THEN 8 WHEN i = 5 THEN 1000 ELSE 4000 + (i % 3000) END, 64, "
                    + "CASE WHEN i % 1000 = 0 THEN 8 WHEN i = 5 THEN 1000 ELSE 4000 + (i % 3000) END, 0 FROM range(1, " + (rows + extraRows + 1) + ") t(i)");
        }
        DuckDBSpatialIndex.Builder builder = DuckDBSpatialIndex.builder(PREFIX);
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("SELECT rowid, wid, x, z FROM co_block WHERE rowid <= " + rows + " ORDER BY rowid")) {
            while (resultSet.next()) {
                builder.add("block", resultSet.getLong(1), resultSet.getInt(2), resultSet.getInt(3), resultSet.getInt(4));
            }
        }
        builder.writeCompleted(connection);
    }

    private long indexedSegments() throws Exception {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM co_duckdb_spatial_index")) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    @Test
    void quietBlockIsNarrowedToItsSegment() throws Exception {
        writeIndexedRows(0);
        assertEquals(3, indexedSegments());
        String[] tables = DuckDBSpatialIndex.boundedTableExpressions(connection, PREFIX, "block", 1, 1000, 1000, 1000, 1000, 2 * SEGMENT, "spatial_rows", "data_rows");
        assertNotNull(tables);
        assertTrue(tables[0].contains("rowid BETWEEN 1 AND " + SEGMENT) && tables[0].endsWith(" AS spatial_rows"), tables[0]);
        assertTrue(tables[1].contains("rowid BETWEEN 1 AND " + SEGMENT) && tables[1].endsWith(" AS data_rows"), tables[1]);
        assertTrue(!tables[0].contains("rowid BETWEEN " + (SEGMENT + 1)), "only the matching segment: " + tables[0]);
    }

    @Test
    void lookupOverTheLimitIsRefused() throws Exception {
        writeIndexedRows(0);
        assertNull(DuckDBSpatialIndex.boundedTableExpressions(connection, PREFIX, "block", 1, 1000, 1000, 1000, 1000, SEGMENT - 1, "spatial_rows", "data_rows"));
    }

    @Test
    void blockInEverySegmentIsRefusedInsteadOfAFullScan() throws Exception {
        writeIndexedRows(0);
        // every segment matches, so the index can't narrow it: a whole-table scan, refused over the limit...
        assertNull(DuckDBSpatialIndex.boundedTableExpressions(connection, PREFIX, "block", 1, 8, 8, 8, 8, 2 * SEGMENT, "spatial_rows", "data_rows"));
        // ...and allowed when the whole table fits in it
        assertNotNull(DuckDBSpatialIndex.boundedTableExpressions(connection, PREFIX, "block", 1, 8, 8, 8, 8, 3 * SEGMENT, "spatial_rows", "data_rows"));
    }

    @Test
    void unindexedTailCountsTowardTheLimit() throws Exception {
        writeIndexedRows(SEGMENT / 2);
        // one indexed segment plus half a segment not yet indexed
        assertNotNull(DuckDBSpatialIndex.boundedTableExpressions(connection, PREFIX, "block", 1, 1000, 1000, 1000, 1000, 2 * SEGMENT, "spatial_rows", "data_rows"));
        assertNull(DuckDBSpatialIndex.boundedTableExpressions(connection, PREFIX, "block", 1, 1000, 1000, 1000, 1000, SEGMENT + SEGMENT / 4, "spatial_rows", "data_rows"));
    }

    @Test
    void sparseRowIdsCountRealRowsNotTheSpan() throws Exception {
        // rowids 4 apart (purged rows, burned sequence values): a segment spans 4 x SEGMENT rowids but holds SEGMENT rows
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO co_block SELECT i * 4, 1, CASE WHEN i = 5 THEN 1000 ELSE 4000 + (i % 3000) END, 64, CASE WHEN i = 5 THEN 1000 ELSE 4000 + (i % 3000) END, 0 FROM range(1, " + (3 * SEGMENT + 1) + ") t(i)");
        }
        DuckDBSpatialIndex.Builder builder = DuckDBSpatialIndex.builder(PREFIX);
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("SELECT rowid, wid, x, z FROM co_block ORDER BY rowid")) {
            while (resultSet.next()) {
                builder.add("block", resultSet.getLong(1), resultSet.getInt(2), resultSet.getInt(3), resultSet.getInt(4));
            }
        }
        builder.writeCompleted(connection);
        assertNotNull(DuckDBSpatialIndex.boundedTableExpressions(connection, PREFIX, "block", 1, 1000, 1000, 1000, 1000, SEGMENT + SEGMENT / 2, "spatial_rows", "data_rows"));
    }

    @Test
    void smallTableWithoutIndexIsScannedWhole() throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO co_block SELECT i, 1, 1, 64, 1, 0 FROM range(1, 101) t(i)");
        }
        String[] tables = DuckDBSpatialIndex.boundedTableExpressions(connection, PREFIX, "block", 1, 1, 1, 1, 1, 1000, "spatial_rows", "data_rows");
        assertNotNull(tables);
        assertEquals(PREFIX + "block AS spatial_rows", tables[0]);
        assertEquals(PREFIX + "block AS data_rows", tables[1]);
        assertNull(DuckDBSpatialIndex.boundedTableExpressions(connection, PREFIX, "block", 1, 1, 1, 1, 1, 99, "spatial_rows", "data_rows"));
    }

    @Test
    void boundedPageQueryRunsAndJoinsTheNarrowedTable() throws Exception {
        writeIndexedRows(0);
        String[] tables = DuckDBSpatialIndex.boundedTableExpressions(connection, PREFIX, "block", 1, 1000, 1000, 1000, 1000, 2 * SEGMENT, "spatial_rows", "data_rows");
        String query = DuckDBLookupQuery.pageQuery(tables, "wid = 1 AND x = 1000 AND z = 1000 AND y = 64", "data_rows.x,data_rows.z", false, 7, 0);
        assertTrue(query.contains("INNER JOIN (SELECT * FROM co_block WHERE rowid BETWEEN"), query);
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(query)) {
            assertTrue(resultSet.next());
            assertEquals(1, resultSet.getLong(1));
            assertEquals(5, resultSet.getLong("result_id"));
        }
    }
}
