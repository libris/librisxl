package whelk

import org.codehaus.jackson.map.ObjectMapper

/**
 * Used to generate a Libris XL index config (index mappings) for ElasticSearch, based on a template
 * (an existing config) and the "display-information" from the viewer.
 */
class ElasticConfigGenerator {

    private final static mapper = new ObjectMapper()
    private final static int cardBoostBase = 500 // Arbitrary number, intended to set card boosts higher than all other boosts.

    /**
     * Generates an ES config (mapping).
     * @param templateString The json TEXT contents of the template
     * @param displayInfoString The json TEXT contents of the display info
     */
    public static String generateConfigString(String templateString,
                                              String displayInfoString) {
        Map displayInfo = mapper.readValue(displayInfoString, Map)
        Map configTemplate = mapper.readValue(templateString, Map)

        Map config = generate(configTemplate, displayInfo)

        return mapper.writeValueAsString(config)
    }

    /**
     * Generates an ES config (mapping).
     * @param configTemplate The json contents of the template as a Map
     * @param displayInfo The json contents of the display info as a Map
     */
    public static Map generate(Map configTemplate, Map displayInfo) {

        Map propertyValues = generatePropertyValues(displayInfo)

        Map categories = configTemplate["mappings"]
        for (category in categories) {

            // Ignore everything except auth, bib and hold
            String key = category.getKey()
            if (key != "auth" && key != "bib" && key != "hold")
                continue

            Map properties = category.getValue()["properties"]
            for (property in properties) {
                if (property.getKey() in propertyValues.keySet()) {
                    boostProperty( property, propertyValues )
                }
            }
        }

        return configTemplate
    }

    /**
     * Add a boosting offset to the given property
     * @param property  The entry in the templateMap (json structure) representing the base of a given property,
     *                  For example:
     *                      "familyName": {
     *                          "analyzer": "completer",
     *                          "type": "string"
     *                      },
     */
    private static void boostProperty(property, propertyValues) {
        Map propertyMap = property.getValue()
        Integer boost = propertyValues[property.getKey()]
        propertyMap["boost"] = boost
    }

    /**
     * Generate a Map, from property names, to the values with which they should be boosted.
     * @param displayInfo The json contents of the display info as a Map
     * @return A map where each property name is mapped to an integer boost value for said property.
     */
    private static Map<String, Integer> generatePropertyValues(Map displayInfo) {

        Map<String, Integer> propertyValues = [:]

        Map categories = displayInfo["lensGroups"]["cards"]["lenses"]
        for (category in categories) {
            Map categoryBody = category.getValue()

            // The boost is decremented by one for each property and RESET for each category.
            int boost = cardBoostBase

            for (property in categoryBody["showProperties"]) {

                String propertyName = property.toString()

                // Has this property already been assigned a boost value, from another category?
                // If so, use the higher one.
                Integer previousBoost = propertyValues[propertyName]
                if (previousBoost == null || previousBoost < boost)
                    propertyValues.put(propertyName, boost--)
            }
        }

        return propertyValues
    }
}
