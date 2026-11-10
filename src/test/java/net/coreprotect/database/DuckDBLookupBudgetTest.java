package net.coreprotect.database;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.coreprotect.config.ConfigHandler;

/** /co lookup, /co near and API lookups with a location are refused when they would read too many rows. */
class DuckDBLookupBudgetTest {

    private static final long SEGMENT = DuckDBSpatialIndex.SEGMENT_ROWS;
    private static final long NOW = 1_800_000_000L;

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
            DuckDBSpatialIndex.createTable("co_", statement);
            // three segments a second apart per row, newest last; a "busy" block in every segment, a "quiet" one in the first
            statement.execute("INSERT INTO co_block SELECT i, " + NOW + " - " + (3 * SEGMENT) + " + i, 1, CASE WHEN i % 1000 = 0 THEN 8 WHEN i = 5 THEN 1000 ELSE 4000 + (i % 3000) END, 64, "
                    + "CASE WHEN i % 1000 = 0 THEN 8 WHEN i = 5 THEN 1000 ELSE 4000 + (i % 3000) END FROM range(1, " + (3 * SEGMENT + 1) + ") t(i)");
        }
        DuckDBSpatialIndex.Builder builder = DuckDBSpatialIndex.builder("co_");
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery("SELECT rowid, wid, x, z FROM co_block ORDER BY rowid")) {
            while (resultSet.next()) {
                builder.add("block", resultSet.getLong(1), resultSet.getInt(2), resultSet.getInt(3), resultSet.getInt(4));
            }
        }
        builder.writeCompleted(connection);
    }

    @AfterEach
    void close() throws Exception {
        DuckDBSpatialIndex.reset();
        connection.close();
        ConfigHandler.databaseType = previousType;
        ConfigHandler.prefix = previousPrefix;
    }

    @Test
    void lookupTheIndexNarrowsRuns() {
        assertTrue(DuckDBLookupQuery.lookupWithinBudget(connection, "block", 1, 995, 1005, 995, 1005, 0L, 0L, SEGMENT + 10));
    }

    @Test
    void lookupThatReadsTheWholeBigTableIsRefused() {
        // the busy block is in every segment: the index can't narrow it, so all 3 segments would be read
        assertFalse(DuckDBLookupQuery.lookupWithinBudget(connection, "block", 1, 0, 16, 0, 16, 0L, 0L, 2 * SEGMENT));
    }

    @Test
    void aShortTimeWindowAtABusySpotStillRuns() {
        // only the newest half segment is inside the window, and DuckDB skips the older row groups
        assertTrue(DuckDBLookupQuery.lookupWithinBudget(connection, "block", 1, 0, 16, 0, 16, NOW - SEGMENT / 2, 0L, 2 * SEGMENT));
    }

    @Test
    void anOldTimeWindowEndStillCounts() {
        // only rows older than the window's end are read: the first half segment fits, the whole table doesn't
        long start = NOW - 3 * SEGMENT;
        assertTrue(DuckDBLookupQuery.lookupWithinBudget(connection, "block", 1, 0, 16, 0, 16, start, start + SEGMENT / 2, 2 * SEGMENT));
        assertFalse(DuckDBLookupQuery.lookupWithinBudget(connection, "block", 1, 0, 16, 0, 16, start, NOW, 2 * SEGMENT));
    }

    @Test
    void radiusArrayIsReadAsMinMaxXAndZ() {
        // {r, minX, maxX, minY, maxY, minZ, maxZ}: the busy spot is refused, the quiet one runs, so x and z come from 1,2,5,6
        assertFalse(DuckDBLookupQuery.lookupWithinBudget(connection, "block", 1, new Integer[] { 5, 0, 16, 60, 70, 0, 16 }, 0L, 0L, 2 * SEGMENT));
        assertTrue(DuckDBLookupQuery.lookupWithinBudget(connection, "block", 1, new Integer[] { 5, 995, 1005, 0, 0, 995, 1005 }, 0L, 0L, SEGMENT + 10));
        assertTrue(DuckDBLookupQuery.lookupWithinBudget(connection, "block", 1, (Integer[]) null, 0L, 0L, 1));
    }

    @Test
    void onlyTheReadTableIsCounted() {
        // the busy block table doesn't refuse a lookup of the empty chat table
        assertTrue(DuckDBLookupQuery.lookupWithinBudget(connection, "chat", 1, 0, 16, 0, 16, 0L, 0L, 2 * SEGMENT));
    }

    @Test
    void otherDatabasesAreNotChecked() {
        ConfigHandler.databaseType = DatabaseType.SQLITE;
        assertTrue(DuckDBLookupQuery.lookupWithinBudget(connection, "block", 1, 0, 16, 0, 16, 0L, 0L, 1));
    }
}
