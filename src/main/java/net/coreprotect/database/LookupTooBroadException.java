package net.coreprotect.database;

/**
 * A DuckDB lookup was refused because it would read more rows than {@link DuckDBLookupQuery#LOOKUP_MAXIMUM_ROWS}.
 * API lookups let it through to the caller instead of returning an empty result, so "refused" isn't read as "nothing
 * logged here".
 */
public final class LookupTooBroadException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public LookupTooBroadException() {
        super("This lookup covers too much logged history to run safely on the DuckDB database. Use a smaller radius or a shorter time.");
    }
}
