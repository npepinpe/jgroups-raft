package org.jgroups.protocols.raft;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.ObjIntConsumer;
import org.jgroups.Address;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.ByteBufferInputStream;
import org.jgroups.util.ByteBufferOutputStream;
import org.jgroups.util.SizeStreamable;
import org.jgroups.util.Util;
import org.lmdbjava.ByteBufferProxy;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.GetOp;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Txn;

/**
 * Optimizations to perform/test: 1. Reusable read transactions 1. Is this supposed to be
 * thread-safe? What's the expected concurrency model here? 1. Buffer pools - depends on concurrency
 * model
 */
@SuppressWarnings("WeakerAccess")
public class LmdbLog implements Log {
  protected static final String STATE_DB_NAME = "state";
  protected static final String LOG_DB_NAME = "log";

  protected org.jgroups.logging.Log logger;
  protected Configuration config;

  protected Env<ByteBuffer> env;
  protected Dbi<ByteBuffer> logDb;
  protected Dbi<ByteBuffer> stateDb;

  protected int currentTerm = 0;
  protected int commitIndex = 0;
  protected int lastAppended = 0;
  protected int firstAppended = 0;
  protected Address votedFor = null;

  public LmdbLog() {}

  @Override
  public void init(final String logName, final Map<String, String> args) throws Exception {
    logger = LogFactory.getLog(String.format("%s-%s", getClass().getName(), logName));
    config = Configuration.parse(logName, args);

    env =
        Env.create(ByteBufferProxy.PROXY_SAFE)
            .setMapSize(config.maxMapSize)
            .setMaxDbs(2)
            .open(
                config.path.toFile(),
                // perform all writes asynchronously to optimize batch writes by flushing manually
                // once a batch is finished
                EnvFlags.MDB_MAPASYNC,
                EnvFlags.MDB_NOMETASYNC,
                EnvFlags.MDB_NOSYNC,
                // allow sharing read transaction across threads
                EnvFlags.MDB_NOTLS);
    stateDb = env.openDbi(STATE_DB_NAME, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
    logDb = env.openDbi(LOG_DB_NAME, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);

    readState();
  }

  @Override
  public void close() {
    if (logDb != null) {
      logDb.close();
    }

    if (stateDb != null) {
      stateDb.close();
    }

    if (env != null) {
      env.close();
    }
  }

  @Override
  public void delete() {
    close();

    try {
      Files.delete(config.path);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public int currentTerm() {
    return currentTerm;
  }

  @Override
  public Log currentTerm(final int currentTerm) {
    if (this.currentTerm == currentTerm) {
      return this;
    }

    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      this.stateDb.put(StateKey.CURRENT_TERM.serialized, serialize(currentTerm));
      commitSync(txn);
      this.currentTerm = currentTerm;
    }

    return this;
  }

  @Override
  public Address votedFor() {
    return votedFor;
  }

  @Override
  public Log votedFor(final Address votedFor) {
    if (this.votedFor.equals(votedFor)) {
      return this;
    }

    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      this.stateDb.put(StateKey.VOTED_FOR.serialized, serialize(votedFor));
      commitSync(txn);
      this.votedFor = votedFor;
    }

    return this;
  }

  @Override
  public int commitIndex() {
    return commitIndex;
  }

  @Override
  public Log commitIndex(final int commitIndex) {
    if (this.commitIndex == commitIndex) {
      return this;
    }

    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      this.stateDb.put(StateKey.COMMIT_INDEX.serialized, serialize(commitIndex));
      commitSync(txn);
      this.commitIndex = commitIndex;
    }

    return this;
  }

  @Override
  public int firstAppended() {
    return firstAppended;
  }

  @Override
  public int lastAppended() {
    return lastAppended;
  }

  @Override
  public void append(final int index, final boolean overwrite, final LogEntry... entries) {
    if (entries.length == 0) {
      return;
    }

    // todo: use overwrite flag to allow overwrite? seems unsafe
    int currentIndex = index;
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer keyBuffer = allocate(Integer.BYTES);
      for (final LogEntry entry : entries) {
        putEntry(txn, keyBuffer.putInt(0, currentIndex), entry);
        currentIndex++;
      }

      commitSync(txn);
    }

    firstAppended = firstAppended == 0 ? index : firstAppended;
    lastAppended = Math.max(lastAppended, index);
  }

  @Override
  public LogEntry get(final int index) {
    try (final Txn<ByteBuffer> txn = getReadTxn()) {
      return get(txn, index).get(); // may throw NoSuchElementException, which is appropriate here
    }
  }

  @Override
  public void truncate(final int index) {
    if (index <= firstAppended) {
      return;
    }

    final int endIndexExclusive = Math.min(commitIndex, index);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final OptionalInt lastDeletedIndex = deleteRange(txn, firstAppended, endIndexExclusive);
      if (lastDeletedIndex.isPresent()) {
        commitSync(txn);
        firstAppended = lastDeletedIndex.getAsInt();
      }
    }
  }

  /**
   * Delete all entries starting from start_index (including the entry at start_index). Updates
   * current_term and last_appended accordingly
   *
   * @param startIndexInclusive
   */
  @Override
  public void deleteAllEntriesStartingFrom(final int startIndexInclusive) {
    if (startIndexInclusive < firstAppended || startIndexInclusive > lastAppended) {
      logger.debug(
          "Ignoring call to deleteAllEntriesStartingFrom with index {} out bounds ([{}, {}])",
          startIndexInclusive,
          firstAppended,
          lastAppended);
      return;
    }

    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final OptionalInt lastDeletedIndex = deleteRange(txn, startIndexInclusive, lastAppended + 1);
      int newLastAppended = lastAppended;
      int newCurrentTerm = currentTerm;
      int newFirstAppended = firstAppended;
      int newCommitIndex = commitIndex;

      if (lastDeletedIndex.isPresent()) {
        final ByteBuffer buffer = allocate(Integer.BYTES);
        newLastAppended = Math.max(0, startIndexInclusive - 1);
        newCommitIndex = Math.min(commitIndex, newLastAppended);
        final Optional<LogEntry> newLastEntry = get(txn, newLastAppended);
        if (newLastEntry.isPresent()) {
          newCurrentTerm = newLastEntry.get().term;
        } else { // log is now empty, so reset everything
          newLastAppended = newCurrentTerm = newFirstAppended = newCommitIndex = 0;
        }

        if (newCurrentTerm != currentTerm) {
          stateDb.put(txn, StateKey.CURRENT_TERM.serialized, buffer.putInt(0, newCurrentTerm));
        }

        if (newCommitIndex != commitIndex) {
          stateDb.put(txn, StateKey.COMMIT_INDEX.serialized, buffer.putInt(0, newCommitIndex));
        }

        commitSync(txn);
      }

      firstAppended = newFirstAppended;
      lastAppended = newLastAppended;
      currentTerm = newCurrentTerm;
      commitIndex = newCommitIndex;
    }
  }

  @Override
  public void forEach(
      final ObjIntConsumer<LogEntry> function,
      final int startIndexInclusive,
      final int endIndexInclusive) {
    final int start = Math.max(startIndexInclusive, firstAppended);
    final int end = Math.min(endIndexInclusive, lastAppended);

    try (final Txn<ByteBuffer> txn = env.txnRead();
        final Cursor<ByteBuffer> cursor = logDb.openCursor(txn)) {
      final ByteBuffer keyBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(start).flip();
      if (!cursor.get(keyBuffer, GetOp.MDB_SET_KEY)) {
        logger.debug("No log entry found at index {}", start);
        return;
      }

      final int index = cursor.key().getInt();
      do {
        function.accept(deserialize(cursor.val()), index);
      } while (cursor.next() && index <= end);
    }
  }

  @Override
  public void forEach(final ObjIntConsumer<LogEntry> function) {
    forEach(function, firstAppended, lastAppended);
  }

  protected Optional<LogEntry> get(final Txn<ByteBuffer> txn, final int index) {
    final ByteBuffer serialized = stateDb.get(txn, serialize(index));
    return Optional.ofNullable(serialized).map(this::deserialize);
  }

  protected void commitSync(final Txn<ByteBuffer> txn) {
    txn.commit();
    env.sync(true);
  }

  protected void readState() {
    try (final Txn<ByteBuffer> txn = getReadTxn();
        final Cursor<ByteBuffer> cursor = logDb.openCursor(txn)) {
      currentTerm = getOrDefault(txn, StateKey.CURRENT_TERM, ByteBuffer::getInt, 0);
      commitIndex = getOrDefault(txn, StateKey.COMMIT_INDEX, ByteBuffer::getInt, 0);
      votedFor = getOrDefault(txn, StateKey.VOTED_FOR, this::<Address>deserialize, null);

      if (cursor.first()) {
        firstAppended = cursor.key().getInt();
      }

      if (cursor.last()) {
        lastAppended = cursor.key().getInt();
      }
    }
  }

  protected <T> T getOrDefault(
      final Txn<ByteBuffer> txn,
      final StateKey key,
      final Function<ByteBuffer, T> deserializer,
      final T defaultValue) {
    return Optional.ofNullable(stateDb.get(txn, key.serialized))
        .map(deserializer)
        .orElse(defaultValue);
  }

  protected Txn<ByteBuffer> getReadTxn() {
    return env.txnRead();
  }

  // todo: handle resetting commitIndex, currentTerm, lastAppended, firstAppended
  protected OptionalInt deleteRange(
      final Txn<ByteBuffer> txn, final int startIndexInclusive, final int endIndexExclusive) {
    // any opened cursors are closed when a transaction is closed, so nothing to worry about here
    final Cursor<ByteBuffer> cursor = logDb.openCursor(txn);
    final ByteBuffer keyBuffer = serialize(startIndexInclusive);
    int lastIndex;

    // returns false iff no key present greater than startIndex, meaning we have nothing to delete
    if (!cursor.get(keyBuffer, GetOp.MDB_SET_KEY)) {
      return OptionalInt.empty();
    }

    do {
      lastIndex = cursor.key().getInt();
      if (lastIndex >= endIndexExclusive) {
        break;
      }
      cursor.delete();
    } while (cursor.next());

    return OptionalInt.of(lastIndex);
  }

  protected void putEntry(final Txn<ByteBuffer> txn, final ByteBuffer key, final LogEntry entry) {
    final ByteBuffer value = serialize(entry);

    // returns false if the key already exists, otherwise throws an exception
    if (!logDb.put(txn, key, value, PutFlags.MDB_APPEND)) {
      logger.trace("Log entry already exists at index {}", key.getInt());
    }
  }

  protected ByteBuffer serialize(final int value) {
    return allocate(Integer.BYTES).putInt(value).flip();
  }

  protected ByteBuffer serialize(final SizeStreamable value) {
    final ByteBuffer buffer = allocate(value.serializedSize());
    final ByteBufferOutputStream outputStream = new ByteBufferOutputStream(buffer);
    try {
      Util.objectToStream(value, outputStream);
    } catch (final Exception e) {
      throwUnchecked(e);
    }

    return buffer;
  }

  protected <T extends SizeStreamable> T deserialize(final ByteBuffer buffer) {
    try {
      return Util.objectFromStream(new ByteBufferInputStream(buffer));
    } catch (final Exception e) {
      throwUnchecked(e);
      return null; // unreachable
    }
  }

  protected static ByteBuffer allocate(final int size) {
    return ByteBuffer.allocateDirect(size);
  }

  @SuppressWarnings("unchecked")
  protected static <T extends Throwable> void throwUnchecked(final Throwable t) throws T {
    throw (T) t;
  }

  // for backwards compatibility, keep track of values and ensure they are never overwritten or
  // changed
  enum StateKey {
    CURRENT_TERM(0),
    COMMIT_INDEX(1),
    VOTED_FOR(2);

    protected final int value;
    protected final ByteBuffer serialized;

    StateKey(final int value) {
      this.value = value;
      this.serialized = allocate(Integer.BYTES).putInt(ordinal()).flip();
    }
  }

  static class Configuration {
    static final String MAX_MAP_SIZE_PROP = "org.jgroups.protocols.raft.LmdbLog.maxMapSize";
    static final String PATH_PROP = "org.jgroups.protocols.raft.LmdbLog.path";

    static final long DEFAULT_MAX_MAP_SIZE = 2L * 1024 * 1024 * 1024; // 2GiB

    private final long maxMapSize;
    private final Path path;

    Configuration(final long maxMapSize, final Path path) {
      this.maxMapSize = maxMapSize;
      this.path = path;
    }

    static Configuration parse(final String logName, final Map<String, String> args) {
      final String pathProp = args.get(PATH_PROP);
      final long maxMapSize =
          Optional.ofNullable(args.get(MAX_MAP_SIZE_PROP))
              .map(Long::valueOf)
              .orElse(DEFAULT_MAX_MAP_SIZE);
      final Path path;

      try {
        if (pathProp != null) {
          path = Files.createDirectories(Paths.get(pathProp));
        } else {
          path = Files.createTempDirectory(logName);
        }
      } catch (final IOException e) {
        throw new UncheckedIOException(e);
      }

      return new Configuration(maxMapSize, path);
    }

    long getMaxMapSize() {
      return maxMapSize;
    }

    Path getPath() {
      return path;
    }
  }
}
