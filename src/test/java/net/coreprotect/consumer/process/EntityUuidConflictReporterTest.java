package net.coreprotect.consumer.process;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class EntityUuidConflictReporterTest {
    private final AtomicLong clock = new AtomicLong(100);
    private final List<String> warnings = new ArrayList<>();
    private final EntityUuidConflictReporter reporter = new EntityUuidConflictReporter(clock::get, warnings::add);
    private final UUID uuid = UUID.fromString("00000000-0000-0000-0000-000000000001");

    @Test
    void firstConflictIsReportedAndDifferentUuidsShareOneRateLimit() {
        reporter.record(false, uuid);
        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains("1 entity interaction"));
        assertTrue(warnings.get(0).contains(uuid.toString()));
        for (int i = 0; i < 1000; i++) reporter.record(false, new UUID(0, i + 10));
        clock.addAndGet(TimeUnit.MINUTES.toNanos(5) - 1);
        reporter.flush();
        assertEquals(1, warnings.size());
        clock.incrementAndGet();
        reporter.flush();
        assertEquals(2, warnings.size());
        assertTrue(warnings.get(1).contains("1000 entity interaction"));
        assertFalse(warnings.get(1).contains("Exception"));
    }

    @Test
    void summarySeparatesRemovalLossAndInteractionLossWithoutRepeatingOldCounts() {
        reporter.record(false, uuid);
        reporter.record(false, uuid);
        reporter.record(true, uuid);
        reporter.record(true, uuid);
        clock.addAndGet(TimeUnit.MINUTES.toNanos(5));
        reporter.flush();
        assertTrue(warnings.get(1).contains("1 entity interaction"));
        assertTrue(warnings.get(1).contains("2 entity removal"));
        clock.addAndGet(TimeUnit.MINUTES.toNanos(5));
        reporter.flush();
        assertEquals(2, warnings.size(), "No reminders when there are no new drops");
        reporter.record(true, uuid);
        assertEquals(3, warnings.size());
        assertTrue(warnings.get(2).contains("1 entity removal"));
        assertFalse(warnings.get(2).contains("interaction"));
    }
}
