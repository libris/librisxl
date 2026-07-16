package whelk.util;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class PropertyLoader {

    static final String SYSTEM_PROPERTY_PREFIX = "xl.";
    static final String PROPERTY_EXTENSION = ".properties";

    private static final HashMap<String, String> userEnteredProperties = new HashMap<>();

    /**
     * MUST be called before loadProperties to have any effect.
     */
    public static void setUserEnteredProperties(String name, String propString) {
        userEnteredProperties.put(name, propString);
    }

    public static Properties loadProperties(String... propNames) {
        Properties props = new Properties();

        /* Order of priority:
        1. Environment parameter pointing to a file, like so -Dxl.secret.properties=./secret.properties
        2. User entered parameters, registered through a previous call to setUserEnteredProperties()
        3. Properties files on the classpath (for example in src/main/resources/secret.properties)
         */

        for (String propName : propNames) {
            InputStream propStream = null;
            boolean systemProperty = false;
            if (System.getProperty(SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION) != null) {
                systemProperty = true;
                try {
                    propStream = new FileInputStream(System.getProperty(SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION));
                } catch (FileNotFoundException ignored) {
                }
            } else if (userEnteredProperties.containsKey(propName)) {
                String propString = userEnteredProperties.get(propName);
                propStream = new ByteArrayInputStream(propString.getBytes());
            } else {
                propStream = PropertyLoader.class.getClassLoader().getResourceAsStream(propName + PROPERTY_EXTENSION);
            }
            if (propStream == null) {
                if (systemProperty) {
                    System.err.println("System property '" + SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION
                            + "' points to non existent file: \"" + System.getProperty(SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION) + "\".");
                } else {
                    System.err.println("No system property '" + SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION
                            + "' specified and no file named " + propName + PROPERTY_EXTENSION + " found in classpath.");
                }
                throw new RuntimeException("Unable to load " + propName + " properties.");
            }
            try (InputStream is = propStream) {
                props.load(is);
            } catch (IOException e) {
                throw new RuntimeException("Unable to load " + propName + " properties.", e);
            }
        }

        // Let system properties prefixed with "xl." override loaded properties.
        // E.g. -Dxl.sqlMaxPoolSize=20 overrides the sqlMaxPoolSize property.
        for (String k : System.getProperties().stringPropertyNames()) {
            if (k.startsWith(SYSTEM_PROPERTY_PREFIX)) {
                String unprefixed = k.substring(SYSTEM_PROPERTY_PREFIX.length());
                if (!unprefixed.endsWith(PROPERTY_EXTENSION)) {
                    props.setProperty(unprefixed, System.getProperty(k));
                }
            }
        }

        return props;
    }
}
