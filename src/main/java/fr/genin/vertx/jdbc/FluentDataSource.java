package fr.genin.vertx.jdbc;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.Shareable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Fluent DataSource pool.
 */
public class FluentDataSource implements RegisteredDs.RegisteredDsStep1,
        RegisteredDs.RegisteredDsStep2, RegisteredDs.RegisteredDsStep3 {
    private static final Logger LOG = Logger.getLogger(FluentDataSource.class.getName());

    public static final String LOCAL_MAP = FluentDataSource.class.getName() + ".local.map";

    private String driverClass;
    private String url;
    private String name;
    private Optional<String> username = Optional.empty();
    private Optional<String> pwd = Optional.empty();

    FluentDataSource() {

    }

    @Override
    public FluentDataSource withName(String name) {
        Objects.nonNull(name);
        this.name = name;
        return this;
    }

    @Override
    public FluentDataSource withUrl(String url) {
        Objects.nonNull(url);
        this.url = url;
        return this;
    }

    @Override
    public FluentDataSource withDriver(String driver) {
        Objects.nonNull(driver);
        this.driverClass = driver;
        return this;
    }

    @Override
    public RegisteredDs.RegisteredDsStep3 withUsername(String username) {
        this.username = Optional.ofNullable(username);
        return this;
    }

    @Override
    public RegisteredDs.RegisteredDsStep3 withPassword(String pwd) {
        this.pwd = Optional.of(pwd);
        return this;
    }

    @Override
    public String register(final Vertx vertx) {
        return register(vertx, UUID.randomUUID().toString());
    }

    @Override
    public String register(final Vertx vertx, final String dsName) {
        Objects.nonNull(vertx);
        Objects.nonNull(dsName);
        try {
            final int processors = Runtime.getRuntime().availableProcessors();
            Class.forName(driverClass);
            final BoneCPConfig config = new BoneCPConfig();
            config.setCloseConnectionWatch(true);
            config.setPartitionCount(processors);
            config.setMinConnectionsPerPartition(10);
            config.setMaxConnectionsPerPartition(100);
            final BoneCPDataSource ds = new BoneCPDataSource(config);
            ds.setJdbcUrl(url);
            username.ifPresent(ds::setUsername);
            pwd.ifPresent(ds::setPassword);

            synchronized (vertx) {
                final LocalMap<Object, Object> localMap = vertx.sharedData().getLocalMap(LOCAL_MAP);
                final SharedDataSource remove = (SharedDataSource) localMap.remove(dsName);
                if (remove != null) {
                    LOG.info("unregister holder datasource" + dsName);
                    remove.dataSource.close();
                }
                localMap.put(dsName, new SharedDataSource(ds));
            }

            return dsName;
        } catch (Exception e) {
            throw new IllegalStateException("Error in creating shared datasource", e);
        }

    }

    static Connection getUniqueConnection(Vertx vertx) {
        Objects.nonNull(vertx);
        synchronized (vertx) {
            final LocalMap<Object, Object> localMap = vertx.sharedData().getLocalMap(LOCAL_MAP);
            return localMap.values().stream().findFirst()
                    .map(o -> ((SharedDataSource) o).connection()).orElseThrow(() -> new IllegalStateException("No Datasource found "));
        }
    }

    static Connection getConnection(Vertx vertx, String dsName) {
        Objects.nonNull(vertx);
        Objects.nonNull(dsName);
        synchronized (vertx) {
            final LocalMap<Object, Object> localMap = vertx.sharedData().getLocalMap(LOCAL_MAP);
            return Optional.ofNullable(localMap.get(dsName))
                    .map(o -> ((SharedDataSource) o).connection()).orElseThrow(() -> new IllegalStateException("No Datasource found with name " + dsName));
        }
    }

    static RegisteredDs.ApiDs api(Vertx vertx, String dsName) {
        Objects.nonNull(vertx);
        Objects.nonNull(dsName);
        synchronized (vertx) {
            final LocalMap<Object, Object> localMap = vertx.sharedData().getLocalMap(LOCAL_MAP);
            final SharedDataSource o = (SharedDataSource) Optional.ofNullable(localMap.get(dsName))
                    .orElseThrow(() -> new IllegalStateException("No Datasource found with name " + dsName));
            return o;
        }
    }


    public static final class SharedDataSource implements Shareable, RegisteredDs.ApiDs {
        private final BoneCPDataSource dataSource;

        public SharedDataSource(BoneCPDataSource dataSource) {
            this.dataSource = dataSource;
        }

        public Connection connection() {
            try {
                return dataSource.getConnection();
            } catch (SQLException e) {
                throw new IllegalStateException("No connection ", e);
            }
        }

        @Override
        public void close() {
            dataSource.close();
        }

        @Override
        public FluentDb.FluentConnection getConnection() {
            return FluentDb.getConnection(connection());
        }
    }

}
