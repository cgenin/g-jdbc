package fr.genin.vertx.jdbc;

import io.vertx.core.Vertx;

/**
 * Interface for creating DS.
 */
public class RegisteredDs {


    public interface ApiDs {
        FluentDb.FluentConnection getConnection();

        void close();

    }

    public interface RegisteredDsStep1 {

        RegisteredDsStep2 withDriver(String driver);
    }

    public interface RegisteredDsStep2 {
        RegisteredDsStep3 withUrl(String url);
    }

    public interface RegisteredDsStep3 {
        RegisteredDsStep3 withName(String name);

        RegisteredDsStep3 withUsername(String username);

        RegisteredDsStep3 withPassword(String pwd);

        String register(final Vertx vertx);

        String register(final Vertx vertx,final String dsName );
    }
}
