package whelk

import org.codehaus.jackson.map.ObjectMapper

/**
 * Used to generate a Libris XL index config (index mappings) for ElasticSearch, based on a template
 * (an existing config) and the "display-information" from the viewer.
 */
class ElasticConfigGenerator {

    private final static mapper = new ObjectMapper()
    private final static int cardBoost = 500 // Arbitrary number, intended to set card boosts higher than all other boosts.

    /**
     * Generates an ES config (mapping).
     * @param templateString The json TEXT contents of the template
     * @param displayInfoString The json TEXT contents of the display info
     */
    public static String generate(String templateString, String displayInfoString) {
        Set cardProperties = parseCardProperties(displayInfoString)

        Map templateMap = mapper.readValue(templateString, Map)

        Map categories = templateMap["mappings"]
        for (category in categories) {

            // Ignore everything except auth, bib and hold
            String key = category.getKey()
            if (key != "auth" && key != "bib" && key != "hold")
                continue

            Map properties = category.getValue()["properties"]["about"]["properties"]
            for (property in properties) {
                if (property.getKey() in cardProperties)
                {
                    boostProperty( property )
                }
            }
        }

        return mapper.writeValueAsString(templateMap)
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
    private static void boostProperty(property) {
        Map propertyMap = property.getValue()
        def existingBoost = propertyMap["boost"]
        int boost = cardBoost
        if (existingBoost)
            boost += existingBoost
        propertyMap["boost"] = boost
    }

    /**
     * Parse the display info and extract a set of all properties that are "card properties".
     * @param displayInfoString The json TEXT contents of the display info
     * @return Set of card properties
     */
    private static Set parseCardProperties(String displayInfoString) {
        Map displayMap = mapper.readValue(displayInfoString, Map)
        Set cardProperties = []

        Map categories = displayMap["lensGroups"]["cards"]["lenses"]
        for (category in categories) {
            Map categoryBody = category.getValue()
            cardProperties.addAll( categoryBody["showProperties"] )
        }

        return cardProperties
    }
}
