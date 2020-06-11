package whelk.util

/**
 * Created by marsko on 2015-11-30.
 */
class PropertyLoader {

    static final String SYSTEM_PROPERTY_PREFIX = "xl."
    static final String PROPERTY_EXTENSION = ".properties"

    private static HashMap<String, String> userEnteredProperties = [:]

    /**
     * MUST be called before loadProperties to have any effect.
     */
    public static void setUserEnteredProperties(String name, String propString) {
        userEnteredProperties.put(name, propString)
    }

    static Properties loadProperties(String... propNames) {
        Properties props = new Properties()

        /* Order of priority:
        1. Environment parameter pointing to a file, like so -Dxl.secret.properties=./secret.properties
        2. Properties files on the classpath (for example in src/main/resources/secret.properties)
        3. User entered parameters, registered through a previous call to setUserEnteredProperties()
         */

        for (propName in propNames) {
            InputStream propStream
            boolean systemProperty = false
            if (System.getProperty(SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION)) {
                systemProperty = true
                propStream = new FileInputStream(System.getProperty(SYSTEM_PROPERTY_PREFIX + propName + PROPERTY_EXTENSION))
            } else if (PropertyLoader.class.getClassLoader().getResourceAsStream(propName + PROPERTY_EXTENSION) != null) {
                propStream = PropertyLoader.class.getClassLoader().getResourceAsStream(propName + PROPERTY_EXTENSION)
            } else if (userEnteredProperties.containsKey(propName)) {
                String propString = userEnteredProperties.get(propName)
                propStream = new ByteArrayInputStream(propString.getBytes())
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
