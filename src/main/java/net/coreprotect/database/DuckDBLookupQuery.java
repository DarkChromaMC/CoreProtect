package net.coreprotect.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import org.bukkit.command.CommandSender;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.utility.Color;
import net.coreprotect.utility.ErrorReporter;

public final class DuckDBLookupQuery {

    /**
     * Most rows one inspector lookup may scan in a table (64 index segments by default). Above it the lookup is
     * refused: a whole-table DuckDB scan fills the DuckDB memory limit and can push the server over its container
     * limit. -Dcoreprotect.duckdb.inspect-max-rows sets it.
     */
    public static final long INSPECT_MAXIMUM_ROWS = Long.getLong("coreprotect.duckdb.inspect-max-rows", 64L * DuckDBSpatialIndex.SEGMENT_ROWS);

    private static final String INSPECT_TOO_BROAD = Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- This spot has too much logged history to inspect safely.";

    /**
     * Most rows a /co lookup, /co near or API lookup with a location may have to read in one table (256 index segments by
     * default; the time window counts, so a short lookup at a busy spot still runs). -Dcoreprotect.duckdb.lookup-max-rows
     * sets it. Rollbacks and restores aren't checked.
     */
    public static final long LOOKUP_MAXIMUM_ROWS = Long.getLong("coreprotect.duckdb.lookup-max-rows", 256L * DuckDBSpatialIndex.SEGMENT_ROWS);

    public static final String LOOKUP_TOO_BROAD = Color.DARK_AQUA + "CoreProtect " + Color.WHITE + "- This lookup covers too much logged history to run safely. Use a smaller radius or a shorter time.";

    /** The tables a location lookup reads (the spatially indexed ones). */
    private static final String[] LOCATION_TABLES = { "block", "container", "item", "sign", "chat", "command", "session" };

    /**
     * True when a DuckDB lookup in this box would read at most {@link #LOOKUP_MAXIMUM_ROWS} rows in each table: the rows
     * the spatial index can't rule out (the whole table when it can't narrow) that are newer than timeStart (0 = all
     * time). The count stops at the limit, and DuckDB skips row groups outside the time window, so it stays cheap.
     */
    public static boolean lookupWithinBudget(Connection connection, int worldId, int minimumX, int maximumX, int minimumZ, int maximumZ, long timeStart) {
        return lookupWithinBudget(connection, worldId, minimumX, maximumX, minimumZ, maximumZ, timeStart, LOOKUP_MAXIMUM_ROWS);
    }

    static boolean lookupWithinBudget(Connection connection, int worldId, int minimumX, int maximumX, int minimumZ, int maximumZ, long timeStart, long maximumRows) {
        if (!ConfigHandler.databaseType.isDuckDB()) {
            return true;
        }
        for (String table : LOCATION_TABLES) {
            String source = DuckDBSpatialIndex.tableExpression(connection, ConfigHandler.prefix, table, worldId, minimumX, maximumX, minimumZ, maximumZ, null, null, "budget_rows");
            String sql = "SELECT COUNT(*) FROM (SELECT 1 FROM " + source + (timeStart > 0 ? " WHERE time >= " + timeStart : "") + " LIMIT " + (maximumRows + 1L) + ")";
            try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
                if (resultSet.next() && resultSet.getLong(1) > maximumRows) {
                    return false;
                }
            }
            catch (SQLException exception) {
                // a failed check doesn't block staff: the lookup runs as it always did
                ErrorReporter.report(exception);
            }
        }
        return true;
    }

    /** {@link #lookupWithinBudget} for a /co lookup radius ({r, minX, maxX, minY, maxY, minZ, maxZ, ...}). */
    public static boolean lookupWithinBudget(Connection connection, int worldId, Integer[] radius, long timeStart) {
        return radius == null || radius.length < 7 || lookupWithinBudget(connection, worldId, radius[1], radius[2], radius[5], radius[6], timeStart);
    }

    /** The lookup refusal; like {@link #refuseInspect} it forgets the sender's previous lookup. */
    public static String refuseLookup(CommandSender commandSender) {
        refuseInspect(commandSender);
        return LOOKUP_TOO_BROAD;
    }

    private DuckDBLookupQuery() {
        throw new IllegalStateException("Database class");
    }

    public static String spatialTable(Connection connection, String table, int worldId, int minimumX, int maximumX, int minimumZ, int maximumZ, String alias) {
        if (!ConfigHandler.databaseType.isDuckDB()) {
            return ConfigHandler.prefix + table + alias(alias);
        }
        return DuckDBSpatialIndex.tableExpression(connection, ConfigHandler.prefix, table, worldId, minimumX, maximumX, minimumZ, maximumZ, null, null, alias);
    }

    public static String entityTable(Connection connection, String table, Collection<Integer> entitySpawnRowIds, String alias) {
        if (!ConfigHandler.databaseType.isDuckDB()) {
            return ConfigHandler.prefix + table + alias(alias);
        }
        return DuckDBSpatialIndex.entityTableExpression(connection, ConfigHandler.prefix, table, entitySpawnRowIds, alias);
    }

    /**
     * The source and data tables for a DuckDB inspector page ({@code spatial_rows} and {@code data_rows}), both narrowed
     * by the spatial index, or null when the lookup would scan more than {@link #INSPECT_MAXIMUM_ROWS}.
     */
    public static String[] inspectTables(Connection connection, String table, int worldId, int minimumX, int maximumX, int minimumZ, int maximumZ) {
        return DuckDBSpatialIndex.boundedTableExpressions(connection, ConfigHandler.prefix, table, worldId, minimumX, maximumX, minimumZ, maximumZ, INSPECT_MAXIMUM_ROWS, "spatial_rows", "data_rows");
    }

    public static String[] inspectEntityTables(Connection connection, String table, Collection<Integer> entitySpawnRowIds) {
        return DuckDBSpatialIndex.boundedEntityTableExpressions(connection, ConfigHandler.prefix, table, entitySpawnRowIds, INSPECT_MAXIMUM_ROWS, "spatial_rows", "data_rows");
    }

    /** True when the whole table is small enough for an inspector lookup that can't use the spatial index. */
    public static boolean inspectWholeTable(Connection connection, String table) {
        return DuckDBSpatialIndex.boundedTableExpressions(connection, ConfigHandler.prefix, table, null, INSPECT_MAXIMUM_ROWS, null, null) != null;
    }

    /** The refusal message; also forgets the sender's previous lookup so paging doesn't continue an older one. */
    public static String refuseInspect(CommandSender commandSender) {
        String name = commandSender.getName();
        ConfigHandler.lookupType.remove(name);
        ConfigHandler.lookupCommand.remove(name);
        ConfigHandler.lookupPage.remove(name);
        ConfigHandler.lookupEntityContainer.remove(name);
        ConfigHandler.lookupEntityInteraction.remove(name);
        return INSPECT_TOO_BROAD;
    }

    public static String pageQuery(String sourceTable, String baseTable, String where, String columns, boolean orderByTime, int limit, int offset) {
        return pageQuery(new String[] { sourceTable, baseTable + " data_rows" }, where, columns, orderByTime, limit, offset);
    }

    /** A page query over inspectTables' pair: the page rows are joined back to the narrowed table, not the whole one. */
    public static String pageQuery(String[] tables, String where, String columns, boolean orderByTime, int limit, int offset) {
        String matchingColumns = orderByTime ? "rowid,time" : "rowid";
        String pageOrder = orderByTime ? "time DESC,rowid DESC" : "rowid DESC";
        String resultOrder = orderByTime ? "page_rows.time DESC NULLS LAST,page_rows.rowid DESC NULLS LAST" : "page_rows.rowid DESC NULLS LAST";
        return "WITH matching_rows AS MATERIALIZED (SELECT " + matchingColumns + " FROM " + tables[0] + " WHERE " + where + "),"
                + "total_rows AS (SELECT COUNT(*) AS count FROM matching_rows),"
                + "page_rows AS (SELECT " + matchingColumns + " FROM matching_rows ORDER BY " + pageOrder + " LIMIT " + limit + " OFFSET " + offset + ") "
                + "SELECT total_rows.count,page_rows.rowid AS result_id," + columns + " FROM total_rows "
                + "LEFT JOIN (page_rows INNER JOIN " + tables[1] + " ON data_rows.rowid=page_rows.rowid) ON true "
                + "ORDER BY " + resultOrder;
    }

    private static String alias(String alias) {
        return alias == null || alias.isEmpty() ? "" : " AS " + alias;
    }
}
