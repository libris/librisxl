package whelk.util

import whelk.DateUtil
import whelk.Document
import whelk.IdGenerator
import whelk.JsonLd

class LegacyIntegrationTools {

    static final Map<String,Long> BASETIMES = [
        "auth": DateUtil.parseDate("1980-01-01").getTime(),
        "bib": DateUtil.parseDate("1984-01-01").getTime(),
        "hold": DateUtil.parseDate("1988-01-01").getTime()
    ]

    static String generateId(String originalIdentifier) {
        String[] parts = originalIdentifier.split("/")
        long basetime = BASETIMES[parts[1]]
        long numericId = basetime + Integer.parseInt(parts.last())
        return IdGenerator.generate(numericId, originalIdentifier)
    }

    static final String BASE_LIBRARY_URI = "https://libris.kb.se/library/"

    static String legacySigelToUri(String sigel) {
        if (sigel.startsWith(BASE_LIBRARY_URI))
            return sigel
        return BASE_LIBRARY_URI + sigel
    }

    static String uriToLegacySigel(String uri) {
        if (uri.startsWith(BASE_LIBRARY_URI))
            return uri.substring(BASE_LIBRARY_URI.length())
        return null
    }

    /**
     * Will return "auth", "bib", "hold" or null
     */
    static String determineLegacyCollection(Document document, JsonLd jsonld) {
        String type = document.getThingType() // for example "Instance"

        String categoryId = getMarcCategoryInHierarchy(type, jsonld)
        return categoryUriToTerm(categoryId)
    }

    static String getMarcCategoryInHierarchy(String type, JsonLd jsonld) {
        def termMap = jsonld.vocabIndex[type]
        if (termMap == null)
            return null

        if (termMap["category"] == null) {
            if (termMap["subClassOf"] != null) {
                List superClasses = termMap["subClassOf"]

                for (superClass in superClasses) {
                    if (superClass == null || superClass["@id"] == null) {
                        continue
                    }
                    String superClassType = jsonld.toTermKey( superClass["@id"] )
                    String category = getMarcCategoryInHierarchy(superClassType, jsonld)
                    if ( category != null )
                        return category
                }
            }
            return null
        }
        else
            return termMap["category"]["@id"]
    }

    static String categoryUriToTerm(String uri) {
        if (uri == null)
            return null
        switch (uri) {
            case "https://id.kb.se/marc/auth":
                return "auth"
            case "https://id.kb.se/marc/bib":
                return "bib"
            case "https://id.kb.se/marc/hold":
                return "hold"
            default: return null
        }
    }

    /**
     * Tomcat incorrectly strips away double slashes from the pathinfo. Compensate here.
     */
    static String fixUri(String uri) {
        if (uri ==~ "/http:/[^/].+") {
            uri = uri.replace("http:/", "http://")
        } else if (uri ==~ "/https:/[^/].+") {
            uri = uri.replace("https:/", "https://")
        }
        return uri
    }
}
