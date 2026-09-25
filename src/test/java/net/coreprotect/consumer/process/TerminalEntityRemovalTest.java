package net.coreprotect.consumer.process;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.bukkit.Location;
import org.bukkit.World;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.database.Database;
import net.coreprotect.database.DatabaseType;
import net.coreprotect.model.entity.EntityInteractionOrigin;
import net.coreprotect.model.entity.EntitySpawnData;
import net.coreprotect.utility.ErrorReporter;

class TerminalEntityRemovalTest {
    @TempDir Path directory;
    private Connection observer;
    private Connection writer;
    private Location location;
    private MockedStatic<Database> database;
    private MockedStatic<ErrorReporter> reporter;
    private final List<Throwable> failures = new ArrayList<>();
    private int failRollbackFrom = Integer.MAX_VALUE;
    private int rollbacks;
    private final UUID problem = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private final UUID before = UUID.fromString("00000000-0000-0000-0000-000000000002");
    private final UUID after = UUID.fromString("00000000-0000-0000-0000-000000000003");

    @BeforeEach
    void setUp() throws Exception {
        ConfigHandler.databaseType = DatabaseType.DUCKDB;
        ConfigHandler.prefix = "co_";
        ConfigHandler.worlds.put("test", 1);
        ConfigHandler.playerIdCache.put("#test", 1);
        Consumer.initialize();
        World world = mock(World.class);
        when(world.getName()).thenReturn("test");
        location = new Location(world, 40, 65, 60, 90, 15);
        String url = "jdbc:duckdb:" + directory.resolve("test.duckdb");
        observer = DriverManager.getConnection(url);
        Database.createDatabaseTables("co_", true, observer, DatabaseType.DUCKDB, false);
        writer = DriverManager.getConnection(url);
        reporter = mockStatic(ErrorReporter.class);
        reporter.when(() -> ErrorReporter.report(any(Throwable.class))).thenAnswer(call -> {
            failures.add(call.getArgument(0));
            return false;
        });
        database = mockStatic(Database.class, CALLS_REAL_METHODS);
        database.when(() -> Database.getConnection(false, 500)).thenReturn(writer);
    }

    @AfterEach
    void tearDown() throws Exception {
        database.close();
        reporter.close();
        writer.close();
        observer.close();
        Consumer.initialize();
        ConfigHandler.worlds.remove("test");
        ConfigHandler.playerIdCache.remove("#test");
    }

    @Test
    void reusesVisibleIdentityAndPreservesHistoryOnRepeatedRemoval() throws Exception {
        insert(problem, 0);
        enqueue(removal(problem));
        enqueue(removal(problem));
        Process.processConsumer(0, false);
        assertRemovedIdentity(problem);
        assertEquals(1, count("SELECT count(*) FROM co_entity_spawn"));
        assertDrained();
        assertTrue(failures.isEmpty(), failures.toString());
    }

    @Test
    void reconcilesIdentityCommittedAfterRemovalSnapshot() throws Exception {
        AtomicInteger inserts = interceptInsert(() -> insert(problem, 0));
        enqueue(removal(problem));
        enqueue(removal(after));
        Process.processConsumer(0, false);
        assertRemovedIdentity(problem);
        assertEquals(1, count("SELECT count(*) FROM co_entity_spawn WHERE uuid='" + after + "' AND removed=1"));
        assertDrained();
        assertTrue(failures.isEmpty(), failures.toString());
        assertEquals(2, inserts.get(), "Only the conflicting attempt and the unrelated terminal insert are needed");
    }

    @Test
    void unresolvableUuidConflictDoesNotTrapFollowingEvents() throws Exception {
        insert(problem, 0);
        // A live old reader retains the unique-index entry after deletion.
        try (Connection reader = DriverManager.getConnection(observer.getMetaData().getURL())) {
            reader.setAutoCommit(false);
            try (Statement statement = reader.createStatement(); ResultSet row = statement.executeQuery("SELECT * FROM co_entity_spawn")) {
                assertTrue(row.next());
            }
            execute("DELETE FROM co_entity_spawn WHERE uuid='" + problem + "'");
            assertEquals(0, count("SELECT count(*) FROM co_entity_spawn"));
            SQLException conflict = assertThrows(SQLException.class, () -> insert(problem, 0));
            assertTrue(conflict.getMessage().contains("Duplicate key \"uuid: "), conflict.getMessage());
            enqueue(removal(before));
            enqueue(removal(problem));
            enqueue(removal(after));
            Process.processConsumer(0, false);
            assertEquals(1, Consumer.consumer.get(0).size(), "Only the following event may remain");
            database.when(() -> Database.getConnection(false, 500)).thenAnswer(call -> DriverManager.getConnection(observer.getMetaData().getURL()));
            Process.processConsumer(0, false);
            assertDrained();
            assertEquals(2, count("SELECT count(*) FROM co_entity_spawn WHERE removed=1"));
            assertEquals(0, count("SELECT count(*) FROM co_entity_spawn WHERE uuid='" + problem + "'"));
            assertEquals(1, failures.size(), failures.toString());
            assertTrue(failures.get(0).getMessage().contains(problem.toString()));
            assertTrue(failures.get(0).getMessage().contains("Dropped entity removal"));
        }
    }

    @Test
    void unrelatedDatabaseFailureRetainsRemovalForRetry() throws Exception {
        interceptInsert(() -> { throw new SQLException("Disk I/O error"); });
        enqueue(removal(problem));
        enqueue(removal(after));
        Process.processConsumer(0, false);
        assertEquals(2, Consumer.consumer.get(0).size());
        assertEquals(0, count("SELECT count(*) FROM co_entity_spawn"));
        assertFalse(Consumer.isPaused);
        assertEquals(1, failures.size());
    }

    @Test
    void alreadyRemovedIdentityKeepsItsRollbackSnapshot() throws Exception {
        insert(problem, 1);
        enqueue(removal(problem));
        Process.processConsumer(0, false);
        assertDrained();
        assertEquals(1, count("SELECT count(*) FROM co_entity_spawn WHERE removed=1 AND x=1 AND data IS NOT NULL AND block_rowid=77 AND kill_rowid=88"));
        assertTrue(failures.isEmpty());
    }

    @Test
    void insertsMissingTerminalIdentityWithImmutableOrigin() throws Exception {
        enqueue(removal(problem));
        Process.processConsumer(0, false);
        assertDrained();
        assertEquals(1, count("SELECT count(*) FROM co_entity_spawn WHERE removed=1 AND time=1234 AND origin_x=10 AND x=40 AND block_rowid IS NULL AND kill_rowid IS NULL"));
        assertTrue(failures.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "Constraint Error: Duplicate key \"kill_rowid: 88\" violates unique constraint.",
            "Constraint Error: Duplicate key \"uuid: 00000000-0000-0000-0000-000000000099\" violates unique constraint."
    })
    void otherConstraintFailuresAreNotDiscarded(String message) throws Exception {
        interceptInsert(() -> { throw new SQLException(message); });
        enqueue(removal(problem));
        Process.processConsumer(0, false);
        assertEquals(1, Consumer.consumer.get(0).size());
        assertFalse(Consumer.isPaused);
        assertEquals(0, count("SELECT count(*) FROM co_entity_spawn"));
        assertFalse(failures.stream().anyMatch(failure -> failure.getMessage().contains("Dropped entity removal")));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2 })
    void failedRollbackNeverRetiresTheRemoval(int failedAttempt) throws Exception {
        insert(problem, 0);
        try (Connection reader = DriverManager.getConnection(observer.getMetaData().getURL())) {
            reader.setAutoCommit(false);
            try (Statement statement = reader.createStatement(); ResultSet row = statement.executeQuery("SELECT * FROM co_entity_spawn")) {
                assertTrue(row.next());
            }
            execute("DELETE FROM co_entity_spawn WHERE uuid='" + problem + "'");
            failRollbackFrom = failedAttempt;
            interceptInsert(() -> { });
            enqueue(removal(problem));
            enqueue(removal(after));
            Process.processConsumer(0, false);
            assertEquals(2, Consumer.consumer.get(0).size());
            assertEquals(0, count("SELECT count(*) FROM co_entity_spawn"));
            assertFalse(Consumer.isPaused);
            assertFalse(failures.stream().anyMatch(failure -> failure.getMessage().contains("Dropped entity removal")));
        }
    }

    private EntitySpawnData removal(UUID uuid) {
        return EntitySpawnData.removed(uuid, new EntityInteractionOrigin(1, 10, 20, 30), location, 1234);
    }

    private void enqueue(EntitySpawnData data) {
        int id = Consumer.consumer.get(0).size();
        Consumer.consumer.get(0).add(new Object[] { id, Process.ENTITY_SPAWN_UPDATE, null, 0, null, 0, 0 });
        Consumer.consumerUsers.get(0).put(id, new String[] { "#test", null });
        Consumer.consumerObjects.get(0).put(id, data);
    }

    private void assertDrained() {
        assertTrue(Consumer.consumer.get(0).isEmpty(), "Consumer must advance past the removal");
        assertTrue(Consumer.consumer.get(1).isEmpty(), "Coordinator must not requeue the failed removal");
        assertFalse(Consumer.isPaused);
    }

    private void assertRemovedIdentity(UUID uuid) throws Exception {
        try (Statement statement = observer.createStatement(); ResultSet row = statement.executeQuery("SELECT * FROM co_entity_spawn WHERE uuid='" + uuid + "'")) {
            assertTrue(row.next());
            assertEquals(1, row.getInt("removed"));
            assertEquals(77, row.getLong("block_rowid"));
            assertEquals(88, row.getInt("kill_rowid"));
            assertEquals(7, row.getInt("time"));
            assertEquals(10, row.getDouble("origin_x"));
            assertEquals(40, row.getDouble("x"));
            assertNull(row.getObject("data"));
            assertFalse(row.next());
        }
    }

    private void insert(UUID uuid, int removed) throws Exception {
        execute("INSERT INTO co_entity_spawn (time,block_rowid,kill_rowid,uuid,wid,current_wid,origin_x,origin_y,origin_z,x,y,z,yaw,pitch,data,removed) VALUES (7,77,88,'" + uuid + "',1,1,10,20,30,1,2,3,0,0,'state'," + removed + ")");
    }

    private void execute(String sql) throws Exception {
        try (Statement statement = observer.createStatement()) { statement.execute(sql); }
    }

    private int count(String sql) throws Exception {
        try (Statement statement = observer.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
            assertTrue(rows.next());
            return rows.getInt(1);
        }
    }

    // Inject only at the JDBC boundary; all queries, constraint errors, transaction
    // recovery, coordinator callbacks and consumer queue cleanup remain real.
    private AtomicInteger interceptInsert(SqlAction action) {
        AtomicInteger calls = new AtomicInteger();
        Connection[] wrapped = new Connection[1];
        Connection proxy = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Connection.class }, (object, method, args) -> {
            try {
                if (method.getName().equals("rollback") && ++rollbacks >= failRollbackFrom) {
                    throw new SQLException("Injected rollback failure");
                }
                Object result = method.invoke(writer, args);
                if (method.getName().equals("createStatement")) {
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Statement.class }, (ignored, operation, parameters) -> {
                        if (operation.getName().equals("getConnection")) {
                            return wrapped[0];
                        }
                        try { return operation.invoke(result, parameters); }
                        catch (InvocationTargetException exception) { throw exception.getCause(); }
                    });
                }
                if (method.getName().equals("prepareStatement") && ((String) args[0]).startsWith("INSERT INTO co_entity_spawn ")) {
                    PreparedStatement actual = (PreparedStatement) result;
                    return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { PreparedStatement.class }, (ignored, operation, parameters) -> {
                        if (operation.getName().equals("executeQuery") && calls.getAndIncrement() == 0) {
                            action.run();
                        }
                        try { return operation.invoke(actual, parameters); }
                        catch (InvocationTargetException exception) { throw exception.getCause(); }
                    });
                }
                return result;
            }
            catch (InvocationTargetException exception) { throw exception.getCause(); }
        });
        wrapped[0] = proxy;
        database.when(() -> Database.getConnection(false, 500)).thenReturn(proxy);
        return calls;
    }

    private interface SqlAction { void run() throws Exception; }
}
