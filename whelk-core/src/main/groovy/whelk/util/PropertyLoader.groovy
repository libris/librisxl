package whelk.util

/**
 * Created by marsko on 2015-11-30.
 */
class PropertyLoader {

    static final String SYSTEM_PROPERTY_PREFIX = "xl."
    static final String PROPERTY_EXTENSION = ".properties"

    static Properties loadProperties(String... propNames) {
        Properties props = new Properties()

        // If an environment parameter is set to point to a file, use that one. Otherwise load from classpath
        for (propName in propNames) {
            InputStream propStream
            boolean systemProperty = false
            if (System.getProperty(SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION)) {
                systemProperty = true
                propStream = new FileInputStream(System.getProperty(SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION))
            } else {
                propStream = PropertyLoader.class.getClassLoader().getResourceAsStream(propName + PROPERTY_EXTENSION)
            }
            if (propStream == null) {
                if (systemProperty) {
                    System.err.println("System property \'${SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION}\' points to non existent file: \"${System.getProperty(SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION)}\".")
                } else {
                    System.err.println("No system property \'${SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION}\' specified and no file named ${propName + PROPERTY_EXTENSION} found in classpath.")
                }
                throw new Exception("Unable to load " + propName + " properties.")
            }
            props.load(propStream)
        }

        return props
    }
}
