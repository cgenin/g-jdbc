package fr.genin.vertx.jdbc;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.math.BigDecimal;
import java.sql.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;

/**
 * Fluent Jdbc api.
 */
public final class FluentDb {
    private static final Logger LOG = Logger.getLogger(FluentDb.class.getName());

    private FluentDb() {
    }

    public static RegisteredDs.RegisteredDsStep1 buildDatasource() {
        return new FluentDataSource();
    }

    public static RegisteredDs.ApiDs datasource(Vertx vertx, String dsname) {
        return FluentDataSource.api(vertx, dsname);
    }

    public static FluentConnection connectionByDataSource(Vertx vertx) {
        return connectionByDataSource(vertx, null);
    }

    public static FluentConnection connectionByDataSource(Vertx vertx, String dsname) {
        Objects.nonNull(vertx);

        final Connection sqlconnection = Optional.ofNullable(dsname).map(name -> FluentDataSource.getConnection(vertx, dsname))
                .orElseGet(() -> FluentDataSource.getUniqueConnection(vertx));
        final FluentConnection connection = getConnection(sqlconnection);
        return connection;
    }

    public static FluentConnection getConnection(final String driverClass, final String url) {
        Objects.nonNull(driverClass);
        Objects.nonNull(url);
        try {
            Class.forName(driverClass);
            final Connection c = DriverManager.getConnection(url);
            return getConnection(c);
        } catch (Exception e) {
            throw new IllegalStateException("Error in creating connection", e);
        }
    }

    public static FluentConnection getConnection(Connection connection) {
        Objects.nonNull(connection);
        return new FluentConnection(connection);
    }


    public static class FluentConnection implements AutoCloseable {
        private static final Logger LOG = Logger.getLogger(FluentConnection.class.getName());

        private final Connection connection;


        private FluentConnection(Connection connection) {
            this.connection = connection;
        }

        public FluentConnection withTransaction() {
            try {
                connection.setAutoCommit(false);
            } catch (SQLException e) {
                LOG.log(Level.SEVERE, "withTransaction", e);
            }
            return this;
        }

        public FluentConnection commit() {
            try {
                connection.commit();
            } catch (SQLException e) {
                LOG.log(Level.SEVERE, "commit", e);
            }
            return this;
        }

        public FluentConnection rollback() {
            try {
                connection.rollback();
            } catch (SQLException e) {
                LOG.log(Level.SEVERE, "rollback", e);
            }
            return this;
        }

        @Override
        public void close() {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.log(Level.SEVERE, "close", e);
            }
//            return this;
        }

        public <T> Optional<T> with(AutoCloseConnection<T> autoCloseConnection) {
            try {
                return autoCloseConnection.map(this);
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "with:", e);
            } finally {
                this.close();
            }
            return Optional.empty();
        }

        public <T> Optional<T> withTransaction(AutoCloseConnection<T> autoCloseConnection) {
            try {
                withTransaction();
                final Optional<T> map = autoCloseConnection.map(this);
                commit();
                return map;
            } catch (Exception e) {
                rollback();
                LOG.log(Level.SEVERE, "with-TRANSACTION:", e);
            } finally {
                this.close();
            }
            return Optional.empty();
        }

        public FluentQuery query() {
            return new FluentQuery(this);
        }

        public FluentBatch batch(String sql, int batchSize) {
            return new FluentBatch(this, sql, batchSize);
        }

        private PreparedStatement getPreparedStatement(String sql) throws Exception {
            Objects.nonNull(sql);
            return connection.prepareStatement(sql);
        }

        private void fillStatement(PreparedStatement statement, JsonArray parameters) throws SQLException {
            if (parameters == null || parameters.size() == 0) {
                return;
            }
            for (int i = 0; i < parameters.size(); i++) {
                statement.setObject(i + 1, parameters.getValue(i));
            }
        }


        private void closeQuietly(PreparedStatement preparedStatement) {
            if (preparedStatement == null) {
                return;
            }
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                LOG.log(Level.FINEST, "preparedstatement-close", e);
            }
        }

        public void closeQuietly(ResultSet resultSet) {
            if (resultSet == null) {
                return;
            }
            try {
                resultSet.close();
            } catch (SQLException e) {
                LOG.log(Level.FINEST, "result-close", e);
            }
        }
    }

    public static class FluentBatch {
        private static final Logger LOG = Logger.getLogger(FluentBatch.class.getName());

        private final FluentConnection connection;
        private final int batchSize;
        private PreparedStatement ps;
        private int counter = 0;

        public FluentBatch(FluentConnection fluentConnection, String sql, int batchSize) {
            connection = fluentConnection;
            this.batchSize = batchSize;
            try {
                ps = connection.getPreparedStatement(sql);
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "with " + sql, e);
            }
        }


        public FluentBatch add(JsonArray jsonArray) {
            Objects.nonNull(ps);
            return add(jsonArray, (v) -> {
            });
        }

        public FluentBatch add(JsonArray jsonArray, Handler<Void> handler) {
            Objects.nonNull(jsonArray);
            try {
                connection.fillStatement(ps, jsonArray);
                ps.addBatch();
                if (++counter % batchSize == 0) {
                    ps.executeBatch();
                }
                handler.handle(null);
            } catch (SQLException e) {
                LOG.log(Level.SEVERE, "add " + jsonArray.encode(), e);
            }
            return this;
        }

        public void end() {
            try {
                ps.executeBatch();
            } catch (SQLException e) {
                LOG.log(Level.SEVERE, "finalize", e);
            } finally {
                connection.closeQuietly(ps);
            }
        }
    }

    public static class FluentQuery {
        private static final Logger LOG = Logger.getLogger(FluentQuery.class.getName());

        private FluentConnection connection;

        private FluentQuery(FluentConnection connection) {
            this.connection = connection;
        }

        public boolean create(String sql) {
            Objects.nonNull(sql);
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = connection.getPreparedStatement(sql);
                preparedStatement.execute();
                return true;
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "insert", e);
            } finally {

                connection.closeQuietly(preparedStatement);

            }
            return false;
        }

        public JsonArray select(String sql, JsonArray jsonArray) {
            Objects.nonNull(sql);
            Objects.nonNull(jsonArray);
            PreparedStatement preparedStatement = null;
            ResultSet resultSet = null;
            try {
                preparedStatement = connection.getPreparedStatement(sql);
                connection.fillStatement(preparedStatement, jsonArray);
                resultSet = preparedStatement.executeQuery();
                return new ResultSetConverter(resultSet).run();

            } catch (Exception e) {
                LOG.log(Level.SEVERE, "select", e);
            } finally {
                connection.closeQuietly(preparedStatement);
                connection.closeQuietly(resultSet);
            }
            return new JsonArray();
        }

        public Optional<Long> insert(String sql, JsonArray jsonArray) {
            Objects.nonNull(sql);
            Objects.nonNull(jsonArray);
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = connection.getPreparedStatement(sql);
                connection.fillStatement(preparedStatement, jsonArray);
                final int nb = preparedStatement.executeUpdate();
                if (nb == 0) {
                    throw new IllegalStateException("Executing failed(" +
                            sql + "), no rows affected.");
                }

                try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        return Optional.of(generatedKeys.getLong(1));
                    } else {
                        LOG.log(Level.FINEST, "Creating user failed, no ID obtained.");
                    }
                }

            } catch (Exception e) {
                LOG.log(Level.SEVERE, "insert", e);
            } finally {

                connection.closeQuietly(preparedStatement);

            }
            return Optional.empty();
        }

    }

    private static class ResultSetConverter {
        private static final Logger LOG = Logger.getLogger(ResultSetConverter.class.getName());
        private final ResultSet resultSet;

        private ResultSetConverter(ResultSet resultSet) {
            this.resultSet = resultSet;
        }

        private Object convertSqlValue(Object value) {
            if (value == null) {
                return null;
            }

            // valid json types are just returned as is
            if (value instanceof Boolean || value instanceof String || value instanceof byte[]) {
                return value;
            }

            // numeric values
            if (value instanceof Number) {
                if (value instanceof BigDecimal) {
                    BigDecimal d = (BigDecimal) value;
                    if (d.scale() == 0) {
                        return ((BigDecimal) value).toBigInteger();
                    } else {
                        // we might loose precision here
                        return ((BigDecimal) value).doubleValue();
                    }
                }

                return value;
            }

            // temporal values
            if (value instanceof Date || value instanceof Time || value instanceof Timestamp) {
                return OffsetDateTime.ofInstant(Instant.ofEpochMilli(((java.util.Date) value).getTime()), ZoneOffset.UTC).format(ISO_OFFSET_DATE_TIME);
            }

            // large objects
            if (value instanceof Clob) {
                Clob c = (Clob) value;
                try {
                    // result might be truncated due to downcasting to int
                    String tmp = c.getSubString(1, (int) c.length());
                    c.free();

                    return tmp;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            if (value instanceof Blob) {
                Blob b = (Blob) value;
                try {
                    // result might be truncated due to downcasting to int
                    byte[] tmp = b.getBytes(1, (int) b.length());
                    b.free();
                    return tmp;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            // arrays
            if (value instanceof Array) {
                Array a = (Array) value;
                try {
                    Object[] arr = (Object[]) a.getArray();
                    if (arr != null) {
                        JsonArray jsonArray = new JsonArray();
                        for (Object o : arr) {
                            jsonArray.add(convertSqlValue(o));
                        }

                        a.free();

                        return jsonArray;
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            // fallback to String
            return value.toString();
        }

        public JsonArray run() {
            try {
                final List<String> columnNames = getColumnName();
                final List<JsonArray> values = getResults(columnNames);
                final JsonArray results = new JsonArray();
                values.stream().map((j) -> {
                    final JsonObject members = new JsonObject();
                    for (int i = 0; i < columnNames.size(); i++) {
                        members.put(columnNames.get(i), j.getValue(i));
                    }
                    return members;
                }).forEach(results::add);
                return results;

            } catch (SQLException e) {
                LOG.log(Level.SEVERE, "run", e);
            }
            return new JsonArray();
        }

        private List<JsonArray> getResults(List<String> columnNames) throws SQLException {
            List<JsonArray> results = new ArrayList<>();

            while (resultSet.next()) {
                JsonArray result = new JsonArray();
                for (int i = 1; i <= columnNames.size(); i++) {
                    Object res = convertSqlValue(resultSet.getObject(i));
                    if (res != null) {
                        result.add(res);
                    } else {
                        result.addNull();
                    }
                }
                results.add(result);
            }
            return results;
        }

        private List<String> getColumnName() throws SQLException {
            List<String> columnNames = new ArrayList<>();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int cols = metaData.getColumnCount();
            for (int i = 1; i <= cols; i++) {
                columnNames.add(metaData.getColumnLabel(i));
            }
            return columnNames;
        }
    }

    @FunctionalInterface
    public interface AutoCloseConnection<T> {

        Optional<T> map(FluentConnection connection) throws Exception;
    }
}
