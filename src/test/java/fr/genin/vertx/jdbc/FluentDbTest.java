package fr.genin.vertx.jdbc;

import static org.junit.Assert.*;

import io.vertx.core.json.JsonArray;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

/**
 * Test.
 */
public class FluentDbTest {
    public static final String CREATETABLE = "createtable";
    public static final String INSERTBATCH = "INSERTBATCH";
    public static final String INSERT = "INSERT";
    public static final String SELECT = "SELECT";
    private String baseDirForTest = "target/jdbcTest/";

    @Before
    public void initPath() throws IOException {
        final File file = new File(baseDirForTest);


        if (!file.exists()) {
            file.mkdirs();
        }

        Files.deleteIfExists(new File(baseDirForTest + CREATETABLE).toPath());
        Files.deleteIfExists(new File(baseDirForTest + INSERTBATCH).toPath());
        Files.deleteIfExists(new File(baseDirForTest + INSERT).toPath());
        Files.deleteIfExists(new File(baseDirForTest + SELECT).toPath());
    }

    @Test
    public void createtable() {
        final FluentDb.FluentConnection connection = FluentDb.getConnection("org.sqlite.JDBC", "jdbc:sqlite:" + baseDirForTest + CREATETABLE);
        assertNotNull(connection);
        final FluentDb.FluentQuery query = connection.query();
        assertNotNull(query);
        final boolean result = query.create("CREATE TABLE t2(a, b INTEGER PRIMARY KEY );");
        assertTrue(result);
        connection.close();
    }


    @Test
    public void createBatchInsert() {
        final FluentDb.FluentConnection connection = FluentDb.getConnection("org.sqlite.JDBC", "jdbc:sqlite:" + baseDirForTest + INSERTBATCH);
        connection.query().create("CREATE TABLE t1(a BIGINT,  b INTEGER PRIMARY KEY);");
        final FluentDb.FluentBatch batch = connection.batch("INSERT INTO t1 (a) VALUES (?);", 5);
        assertNotNull(batch);
        for (int i = 0; i < 100; i++) {
            batch.add(new JsonArray().add(i));
        }
        batch.end();

        connection.close();
    }

    @Test
    public void createInsert() {
        final FluentDb.FluentConnection connection = FluentDb.getConnection("org.sqlite.JDBC", "jdbc:sqlite:" + baseDirForTest + INSERT);
        final FluentDb.FluentQuery query = connection.query();
        query.create("CREATE TABLE t3(a BIGINT,  b INTEGER PRIMARY KEY);");
        final Optional<Long> id = query.insert("INSERT INTO t3 (a) VALUES (?);", new JsonArray().add(200));
        assertTrue(id.isPresent());
        assertTrue(id.get() > 0);
        connection.close();
    }

    @Test
    public void createSelect() {
        final FluentDb.FluentConnection connection = FluentDb.getConnection("org.sqlite.JDBC", "jdbc:sqlite:" + baseDirForTest + SELECT);
        final FluentDb.FluentQuery query = connection.query();
        query.create("CREATE TABLE t4 (a BIGINT,  b INTEGER PRIMARY KEY);");
        query.insert("INSERT INTO t4 (a) VALUES (?);", new JsonArray().add(200));
        final JsonArray select = query.select("SELECT * FROM t4 WHERE a=?;", new JsonArray().add(200));
        assertNotNull(select);
        assertEquals(1, select.size());
        assertEquals("{\"a\":200,\"b\":1}", select.getJsonObject(0).encode());
        connection.close();
    }
}
