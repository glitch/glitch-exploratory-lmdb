package dev.glitch.exploratory.lmdb;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;
import static org.lmdbjava.GetOp.MDB_SET;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_PREV;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lmdbjava.Cursor;
import org.lmdbjava.CursorIterable;
import org.lmdbjava.CursorIterable.KeyVal;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

public class TestApplication {

    private static final String DB_NAME = "myTestLMDB";
    private static Long MAP_SIZE = 10_485_760L; // 10MB for testing
    @Test
    public void testMe() throws IOException {
        Path path = Files.createTempDirectory("testLmdb");
        // We always need an Env. An Env owns a physical on-disk storage file. One
        // Env can store many different databases (ie sorted maps).
        final Env<ByteBuffer> env = Env.create()
                // LMDB also needs to know how large our DB might be. Over-estimating is OK.
                .setMapSize(MAP_SIZE)
                // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
                .setMaxDbs(1)
                // Now let's open the Env. The same path can be concurrently opened and
                // used in different processes, but do not open the same path twice in
                // the same process at the same time.
                .open(path.toFile());

        // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
        // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
        final Dbi<ByteBuffer> db = env.openDbi(DB_NAME, MDB_CREATE);

        // We want to store some data, so we will need a direct ByteBuffer.
        // Note that LMDB keys cannot exceed maxKeySize bytes (511 bytes by default).
        // Values can be larger.
        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        final ByteBuffer val = allocateDirect(700);
        key.put("greeting".getBytes(UTF_8)).flip();
        val.put("Hello world".getBytes(UTF_8)).flip();
        final int valSize = val.remaining();

        // Now store it. Dbi.put() internally begins and commits a transaction (Txn).
        db.put(key, val);

        // To fetch any data from LMDB we need a Txn. A Txn is very important in
        // LmdbJava because it offers ACID characteristics and internally holds a
        // read-only key buffer and read-only value buffer. These read-only buffers
        // are always the same two Java objects, but point to different LMDB-managed
        // memory as we use Dbi (and Cursor) methods. These read-only buffers remain
        // valid only until the Txn is released or the next Dbi or Cursor call. If
        // you need data afterwards, you should copy the bytes to your own buffer.
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            final ByteBuffer found = db.get(txn, key);
            assertNotNull(found);

            // The fetchedVal is read-only and points to LMDB memory
            final ByteBuffer fetchedVal = txn.val();
            Assertions.assertEquals(fetchedVal.remaining(), valSize);

            // Let's double-check the fetched value is correct
            Assertions.assertEquals(UTF_8.decode(fetchedVal).toString(), "Hello world");
        }

        // We can also delete. The simplest way is to let Dbi allocate a new Txn...
        db.delete(key);

        // Now if we try to fetch the deleted row, it won't be present
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            assertNull(db.get(txn, key));
        }

        env.close();
    }

    /**
     * Store 100 randomly generated UUIDs
     * @throws IOException
     */
    @Test
    public void testUUIDs() throws IOException {
        Path dbPath = Paths.get("persistentLMDB");
        Path path = Files.exists(dbPath) ? dbPath : Files.createDirectory(dbPath);
        File dbFile = path.toFile();

        Env<ByteBuffer> env = Env.create()
                .setMapSize(MAP_SIZE)
                .setMaxDbs(1)
                .setMaxReaders(1)
                .open(dbFile);

        final Dbi<ByteBuffer> db = env.openDbi(DB_NAME, MDB_CREATE, MDB_DUPSORT);
        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        final ByteBuffer val = allocateDirect(700);

        for(int i = 0; i < 100; i+=1) {
            key.put(UUID.randomUUID().toString().getBytes(UTF_8)).flip();
            val.put(UUID.randomUUID().toString().getBytes(UTF_8)).flip();
            db.put(key, val);
        }

        db.close();
        
        // Now read db back in
        System.out.println("Attempting to read DB");
        Long counter = 0L;
        final Dbi<ByteBuffer> dbRead = env.openDbi(DB_NAME);
        // All reads must happen through a transaction
        final Txn<ByteBuffer> txn1 = env.txnRead();
        // We're going to use the cursor iterable
        try (CursorIterable<ByteBuffer> ci = dbRead.iterate(txn1, KeyRange.all())) {
        for (final KeyVal<ByteBuffer> kv : ci) {
            System.out.println(UTF_8.decode(kv.key()).toString() + ", " + UTF_8.decode(kv.val()).toString());
            counter += 1;
        }
        txn1.close();
        dbRead.close();
        System.out.println("Total count: " + counter);
      }
    }
}
