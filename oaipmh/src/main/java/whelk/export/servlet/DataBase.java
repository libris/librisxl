package whelk.export.servlet;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;

public class DataBase {

    private static HikariDataSource dataSource;
    private static final Logger logger = LoggerFactory.getLogger(DataBase.class);

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
            logger.error("Incorrect JDBC connection string. Check secret.properties.");
            return;
        }

        final int userAndPassPartBeginsAt = jdbcUrlPrefix.length();
        final int urlPartBeginsAt = rawJdbcUrl.indexOf("@") + 1;

        final String userAndPass = rawJdbcUrl.substring(userAndPassPartBeginsAt, urlPartBeginsAt - 1);
        final String urlPart = rawJdbcUrl.substring(urlPartBeginsAt);

        final String[] userAndPassArray = userAndPass.split(":");
        if (userAndPassArray.length != 2)
        {
            logger.error("Expected username and password as part of the JDBC string. Check secret.properties.");
            return;
        }

        String user;
        String password;
        try {
            user = java.net.URLDecoder.decode(userAndPassArray[0], "UTF-8");
            password = java.net.URLDecoder.decode(userAndPassArray[1], "UTF-8");
        } catch (UnsupportedEncodingException e)
        {
            logger.error("Could not URL decode username/password: " + e);
            return;
        }

        dataSource.setJdbcUrl( jdbcUrlPrefix + urlPart );
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setAutoCommit(false);
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
