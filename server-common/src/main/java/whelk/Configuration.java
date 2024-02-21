package whelk;

import java.nio.file.FileSystems;
import java.nio.file.Path;

public class Configuration {
    public static final String BATCH_THREAD_GROUP = "whelk-batch";

    private static final String HTTP_PORT_PARAMETER = "xl.http.port";
    private static final int DEFAULT_HTTP_PORT = 8180;

    private static final String STATIC_DIR_PARAMETER = "xl.http.static";
    private static final String DEFAULT_STATIC_DIR = "/srv/static";

    private static final String MAX_CONNECTIONS_PARAMETER = "xl.http.maxConnections";
    private static final int DEFAULT_MAX_CONNECTIONS = 500;

    private static final String LOG_ROOT_PARAMETER = "xl.logRoot";
    private static final String DEFAULT_LOG_ROOT = "./logs";

    public static int getHttpPort() {
        return Integer.parseInt(System.getProperty(HTTP_PORT_PARAMETER, "" + DEFAULT_HTTP_PORT));
    }

    public static String getStaticContentDir() {
        return System.getProperty(STATIC_DIR_PARAMETER, DEFAULT_STATIC_DIR);
    }

    public static Path getLogRoot() {
        return FileSystems.getDefault().getPath(System.getProperty(LOG_ROOT_PARAMETER, DEFAULT_LOG_ROOT));
    }

    public static int getMaxConnections() {
        return Integer.parseInt(System.getProperty(MAX_CONNECTIONS_PARAMETER, "" + DEFAULT_MAX_CONNECTIONS));
    }
}
