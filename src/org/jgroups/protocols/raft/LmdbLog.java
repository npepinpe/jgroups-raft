package org.jgroups.protocols.raft;

import java.io.DataInput;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;
import java.util.function.ObjIntConsumer;
import org.jgroups.Address;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.ByteBufferInputStream;
import org.jgroups.util.ByteBufferOutputStream;
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
 * Optimizations to perform/test:
 *  1. Reusable read transactions
 *  1. Is this supposed to be thread-safe? What's the expected concurrency model here?
 *  1. Buffer pools - depends on concurrency model
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

    // initialize state
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
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public int currentTerm() {
    return currentTerm;
  }

  @Override
  public Log currentTerm(int new_term) {
    final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).putInt(new_term).flip();
    stateDb.put(StateKey.CURRENT_TERM.serialized, buffer);

    return this;
  }

  @Override
  public Address votedFor() {
    return votedFor;
  }

  @Override
  public Log votedFor(Address votedFor) {
    final byte[] serialized;
    try {
      serialized = Util.objectToByteBuffer(votedFor);
    } catch (Exception e) {
      throwUnchecked(e);
      return this; // unreachable but compiler doesn't know this
    }

    final ByteBuffer buffer = ByteBuffer.wrap(serialized);
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      this.stateDb.put(StateKey.VOTED_FOR.serialized, buffer);
      txn.commit();
      env.sync(true);
      this.votedFor = votedFor;
    }

    return this;
  }

  @Override
  public int commitIndex() {
    return commitIndex;
  }

  @Override
  public Log commitIndex(int commitIndex) {
    final ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).putInt(commitIndex).flip();
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      this.stateDb.put(StateKey.COMMIT_INDEX.serialized, buffer);

      txn.commit();
      env.sync(true);
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
  public void append(int index, boolean overwrite, LogEntry... entries) {
    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      final ByteBuffer keyBuffer = ByteBuffer.allocate(Integer.BYTES);
      for (final LogEntry entry : entries) {
        put(txn, keyBuffer.putInt(index).flip(), entry);
        index++;
      }

      txn.commit();
      env.sync(true);
    }

    lastAppended = Math.max(lastAppended, index);
  }

  @Override
  public LogEntry get(int index) {
    final ByteBuffer keyBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(index).flip();

    try (final Txn<ByteBuffer> txn = env.txnRead()) {
      final ByteBuffer serialized = stateDb.get(txn, keyBuffer);
      final LogEntry entry = new LogEntry();
      final DataInput inputStream = new ByteBufferInputStream(serialized);
      entry.readFrom(inputStream);

      return entry;
    } catch (Exception e) {
      throwUnchecked(e);
      return null; // unreachable
    }
  }

  @Override
  public void truncate(int index) {
    if (index <= firstAppended) {
      return;
    }

    final int endIndexExclusive = Math.min(commitIndex, index);
    deleteRange(firstAppended, endIndexExclusive);

  }

  @Override
  public void deleteAllEntriesStartingFrom(int startIndexInclusive) {
    if (startIndexInclusive >= lastAppended) {
      return;
    }
  }

  @Override
  public void forEach(
      ObjIntConsumer<LogEntry> function, int startIndexInclusive, int endIndexInclusive) {
    final int start = Math.max(startIndexInclusive, firstAppended);
    final int end = Math.min(endIndexInclusive, lastAppended);

    try (final Txn<ByteBuffer> txn = env.txnRead();
        final Cursor<ByteBuffer> cursor = logDb.openCursor(txn)) {
      final ByteBuffer keyBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(start).flip();
      if (!cursor.get(keyBuffer, GetOp.MDB_SET_KEY)) {
        logger.debug("No log entry found at index {}", start);
        return;
      }

      int index = cursor.key().getInt();
      do {
        function.accept(deserialize(cursor.val()), index);
      } while (cursor.next() && index <= end);
    }
  }

  @Override
  public void forEach(ObjIntConsumer<LogEntry> function) {
    forEach(function, firstAppended, lastAppended);
  }

  // todo: handle resetting commitIndex, currentTerm, lastAppended, firstAppended
  protected void deleteRange(final int startIndexInclusive, final int endIndexExclusive) {
    int lastTerm;
    int lastIndex;

    try (final Txn<ByteBuffer> txn = env.txnWrite()) {
      // closing a write transaction will close any opened cursors
      final Cursor<ByteBuffer> cursor = logDb.openCursor(txn);
      if (startIndexInclusive < firstAppended) {
          if (!cursor.first()) { // empty log
              return;
          }
      } else {
        final ByteBuffer keyBuffer = ByteBuffer.allocate(Integer.BYTES).putInt(startIndexInclusive).flip();
        // returns false iff no key present greater than startIndex, meaning we have nothing to
        // delete
        if (!cursor.get(keyBuffer, GetOp.MDB_SET_KEY)) {
          return;
        }
      }

      do {
        lastIndex = cursor.key().getInt();
        if (lastIndex >= endIndexExclusive) {
          break;
        }
        cursor.delete();
      } while (cursor.next());


      txn.commit();
      env.sync(true);
    }

    firstAppended = lastIndex;

  }

  protected void put(final Txn<ByteBuffer> txn, final ByteBuffer key, final LogEntry entry) {
    final ByteBuffer value = serialize(entry);

    // returns false if the key already exists, otherwise throws an exception
    if (!logDb.put(txn, key, value, PutFlags.MDB_APPEND)) {
      logger.trace("Log entry already exists at index {}", key.getInt());
    }
  }

  protected ByteBuffer serialize(final LogEntry entry) {
    final ByteBuffer buffer = ByteBuffer.allocate(entry.length);
    final ByteBufferOutputStream outputStream = new ByteBufferOutputStream(buffer);
    try {
      entry.writeTo(outputStream);
    } catch (Exception e) {
      throwUnchecked(e);
    }

    return buffer;
  }

  protected LogEntry deserialize(final ByteBuffer buffer) {
    final LogEntry entry = new LogEntry();
    final ByteBufferInputStream inputStream = new ByteBufferInputStream(buffer);
    try {
      entry.readFrom(inputStream);
    } catch (Exception e) {
      throwUnchecked(e);
    }

    return entry;
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
      } catch (IOException e) {
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

  // for backwards compatibility, keep track of values and ensure they are never overwritten or
  // changed
  enum StateKey {
    FIRST_APPENDED(0),
    LAST_APPENDED(1),
    CURRENT_TERM(2),
    COMMIT_INDEX(3),
    VOTED_FOR(4);

    protected final int value;
    protected final ByteBuffer serialized;

    StateKey(final int value) {
      this.value = value;
      this.serialized = ByteBuffer.allocate(Integer.BYTES).putInt(ordinal()).flip();
    }
  }

  @SuppressWarnings("unchecked")
  protected static <T extends Throwable> void throwUnchecked(final Throwable t) throws T {
    throw (T) t;
  }
}
