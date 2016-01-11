package whelk.export.servlet;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DataBase {

    private static HikariDataSource dataSource;

    /**
     * Initialize the db connection pool.
     */
    public static void init()
    {
        /*
        Extracts the components of a JDBC url, and reconstructs it without username and password,
        username and password being passed as parameters instead.
        */

        dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");

        final String jdbcUrlPrefix = "jdbc:postgresql://";
        final String rawJdbcUrl = OaiPmh.configuration.getProperty("sqlUrl");

        if (!rawJdbcUrl.startsWith(jdbcUrlPrefix))
        {
            // Incorrect jdbc string. TODO: LOG!
            return;
        }

        final int userAndPassPartBeginsAt = jdbcUrlPrefix.length();
        final int urlPartBeginsAt = rawJdbcUrl.indexOf("@") + 1;

        final String userAndPass = rawJdbcUrl.substring(userAndPassPartBeginsAt, urlPartBeginsAt - 1);
        final String urlPart = rawJdbcUrl.substring(urlPartBeginsAt);

        final String[] userAndPassArray = userAndPass.split(":");
        if (userAndPassArray.length != 2)
        {
            // Expected username and password in jdbc url. TODO: LOG!
            return;
        }
        final String user = userAndPassArray[0];
        final String password = userAndPassArray[1];

        dataSource.setJdbcUrl( jdbcUrlPrefix + urlPart );
        dataSource.setUsername(user);
        dataSource.setPassword(password);
    }

    public static void destroy()
    {
        dataSource.close();
    }

    public static Connection getConnection() throws SQLException
    {
        return dataSource.getConnection();
    }
}
