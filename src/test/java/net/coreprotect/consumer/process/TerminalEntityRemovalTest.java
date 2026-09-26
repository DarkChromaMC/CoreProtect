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
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.entity.EntityType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

import net.coreprotect.config.ConfigHandler;
import net.coreprotect.config.Config;
import net.coreprotect.consumer.Consumer;
import net.coreprotect.database.Database;
import net.coreprotect.database.DatabaseType;
import net.coreprotect.model.entity.EntityInteractionOrigin;
import net.coreprotect.model.entity.EntityInteraction;
import net.coreprotect.model.entity.EntityInteractionAction;
import net.coreprotect.model.entity.EntitySpawnData;
import net.coreprotect.utility.ErrorReporter;

class TerminalEntityRemovalTest {
    @TempDir Path directory;
    private Connection observer;
    private Connection writer;
    private Location location;
    private World world;
    private MockedStatic<Database> database;
    private MockedStatic<ErrorReporter> reporter;
    private final List<Throwable> failures = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private int stackTraces;
    private Handler warningHandler;
    private boolean originalApiEnabled;
    private boolean originalParentHandlers;
    private int failRollbackFrom = Integer.MAX_VALUE;
    private int rollbacks;
    private SqlAction afterRollback;
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
        originalApiEnabled = Config.getGlobal().API_ENABLED;
        Config.getGlobal().API_ENABLED = false;
        ConfigHandler.entities.put("cow", 1);
        world = mock(World.class);
        when(world.getName()).thenReturn("test");
        location = new Location(world, 40, 65, 60, 90, 15);
        String url = "jdbc:duckdb:" + directory.resolve("test.duckdb");
        observer = DriverManager.getConnection(url);
        Database.createDatabaseTables("co_", true, observer, DatabaseType.DUCKDB, false);
        writer = DriverManager.getConnection(url);
        reporter = mockStatic(ErrorReporter.class);
        reporter.when(() -> ErrorReporter.report(any(Throwable.class))).thenAnswer(call -> {
            stackTraces++;
            failures.add(call.getArgument(0));
            return false;
        });
        reporter.when(() -> ErrorReporter.report(any(Throwable.class), eq(false))).thenAnswer(call -> {
            failures.add(call.getArgument(0));
            return false;
        });
        Logger logger = Logger.getLogger("CoreProtect");
        originalParentHandlers = logger.getUseParentHandlers();
        logger.setUseParentHandlers(false);
        warningHandler = new Handler() {
            @Override public void publish(LogRecord record) { warnings.add(record.getMessage()); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        logger.addHandler(warningHandler);
        database = mockStatic(Database.class, CALLS_REAL_METHODS);
        database.when(() -> Database.getConnection(false, 500)).thenReturn(writer);
    }

    @AfterEach
    void tearDown() throws Exception {
        Logger.getLogger("CoreProtect").removeHandler(warningHandler);
        Logger.getLogger("CoreProtect").setUseParentHandlers(originalParentHandlers);
        Config.getGlobal().API_ENABLED = originalApiEnabled;
        ConfigHandler.entities.remove("cow");
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
            assertEquals(0, stackTraces);
            assertEquals(1, warnings.size());
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
    @CsvSource({ "1,false", "2,false", "1,true", "2,true" })
    void failedRollbackNeverRetiresTheEvent(int failedAttempt, boolean interaction) throws Exception {
        insert(problem, 0);
        try (Connection reader = DriverManager.getConnection(observer.getMetaData().getURL())) {
            reader.setAutoCommit(false);
            try (Statement statement = reader.createStatement(); ResultSet row = statement.executeQuery("SELECT * FROM co_entity_spawn")) {
                assertTrue(row.next());
            }
            execute("DELETE FROM co_entity_spawn WHERE uuid='" + problem + "'");
            failRollbackFrom = failedAttempt;
            interceptInsert(() -> { });
            if (interaction) enqueueInteraction(problem);
            else enqueue(removal(problem));
            enqueue(removal(after));
            Process.processConsumer(0, false);
            assertEquals(2, Consumer.consumer.get(0).size());
            assertEquals(0, count("SELECT count(*) FROM co_entity_spawn"));
            assertFalse(Consumer.isPaused);
            assertFalse(failures.stream().anyMatch(failure -> failure.getMessage().contains("Dropped entity removal")));
        }
    }

    @Test
    void interactionReconcilesConcurrentIdentityAndRecordsBothClicks() throws Exception {
        interceptInsert(() -> insert(problem, 0));
        enqueueInteraction(problem);
        enqueueInteraction(problem);
        Process.processConsumer(0, false);
        assertDrained();
        assertEquals(2, count("SELECT count(*) FROM co_entity_interaction"));
        assertEquals(1, count("SELECT count(*) FROM co_entity_spawn WHERE block_rowid=77 AND kill_rowid=88 AND removed=0 AND x=40"));
        assertEquals(2, count("SELECT count(*) FROM co_entity_interaction i JOIN co_entity_spawn e ON i.entity_spawn_rowid=e.rowid WHERE i.x=10 AND i.y=20 AND i.z=30"));
        assertTrue(failures.isEmpty(), failures.toString());
    }

    @Test
    void interactionRetriesInsertionWhenConflictDisappearsAfterRollback() throws Exception {
        // A genuine concurrent insert aborts the writer; delete it just after
        // successful rollback so the retry must create a new identity.
        interceptInsert(() -> insert(problem, 0));
        afterRollback = () -> execute("DELETE FROM co_entity_spawn WHERE uuid='" + problem + "'");
        enqueueInteraction(problem);
        Process.processConsumer(0, false);
        assertDrained();
        assertEquals(1, count("SELECT count(*) FROM co_entity_interaction"));
        assertEquals(1, count("SELECT count(*) FROM co_entity_spawn WHERE block_rowid IS NULL AND removed=0"));
        assertTrue(failures.isEmpty(), failures.toString());
    }

    @Test
    void repeatedUnresolvableInteractionsEmitOneShortWarningAndDoNotTrapQueue() throws Exception {
        insert(problem, 0);
        try (Connection reader = DriverManager.getConnection(observer.getMetaData().getURL())) {
            reader.setAutoCommit(false);
            try (Statement statement = reader.createStatement(); ResultSet row = statement.executeQuery("SELECT * FROM co_entity_spawn")) {
                assertTrue(row.next());
            }
            execute("DELETE FROM co_entity_spawn WHERE uuid='" + problem + "'");
            for (int i = 0; i < 8; i++) enqueueInteraction(problem);
            enqueue(removal(after));
            database.when(() -> Database.getConnection(false, 500)).thenAnswer(call -> DriverManager.getConnection(observer.getMetaData().getURL()));
            for (int i = 0; i < 9; i++) Process.processConsumer(0, false);
            assertDrained();
            assertEquals(0, count("SELECT count(*) FROM co_entity_interaction"));
            assertEquals(1, count("SELECT count(*) FROM co_entity_spawn WHERE removed=1"));
            assertEquals(8, failures.size(), "Detailed reports remain available without console stack traces");
            assertEquals(0, stackTraces);
            assertEquals(1, warnings.size());
            assertTrue(warnings.get(0).contains("1 entity interaction"));
            assertTrue(warnings.get(0).contains("not saved"));
            assertFalse(warnings.get(0).contains("Exception"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "Disk I/O error",
            "Constraint Error: Duplicate key \"kill_rowid: 88\" violates unique constraint.",
            "Constraint Error: Duplicate key \"uuid: 00000000-0000-0000-0000-000000000099\" violates unique constraint."
    })
    void unexpectedInteractionFailuresRemainVisibleAndRetained(String message) throws Exception {
        interceptInsert(() -> { throw new SQLException(message); });
        enqueueInteraction(problem);
        enqueue(removal(after));
        Process.processConsumer(0, false);
        assertEquals(2, Consumer.consumer.get(0).size());
        assertEquals(0, count("SELECT count(*) FROM co_entity_interaction"));
        assertEquals(1, stackTraces);
        assertTrue(warnings.isEmpty());
        assertFalse(Consumer.isPaused);
    }

    private void enqueueInteraction(UUID uuid) {
        int id = Consumer.consumer.get(0).size();
        EntityInteraction data = new EntityInteraction(uuid, EntityType.COW,
                new EntityInteractionOrigin(1, 10, 20, 30), location,
                EntityInteractionAction.GENERIC, null, 1234);
        Consumer.consumer.get(0).add(new Object[] { id, Process.ENTITY_INTERACTION, null, 0, null, 0, 0 });
        Consumer.consumerUsers.get(0).put(id, new String[] { "#test", null });
        Consumer.consumerObjects.get(0).put(id, data);
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
                if (method.getName().equals("rollback") && afterRollback != null) {
                    SqlAction callback = afterRollback;
                    afterRollback = null;
                    callback.run();
                }
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
