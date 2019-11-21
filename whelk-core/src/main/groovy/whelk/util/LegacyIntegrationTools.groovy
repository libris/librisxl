package whelk.util

import groovy.transform.CompileStatic
import whelk.DateUtil
import whelk.Document
import whelk.IdGenerator
import whelk.JsonLd

@CompileStatic
class LegacyIntegrationTools {

    static final Map<String,Long> BASETIMES = [
        "auth": DateUtil.parseDate("1980-01-01").getTime(),
        "bib": DateUtil.parseDate("1984-01-01").getTime(),
        "hold": DateUtil.parseDate("1988-01-01").getTime()
    ]

    static final Map<String, String> MARC_COLLECTION_BY_CATEGORY = [
        'https://id.kb.se/marc/auth': 'auth',
        'https://id.kb.se/marc/bib': 'bib',
        'https://id.kb.se/marc/hold': 'hold'
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

        return getMarcCollectionInHierarchy(type, jsonld)
    }

    static String getMarcCollectionInHierarchy(String type, JsonLd jsonld) {
        Map termMap = jsonld.vocabIndex[type]
        if (termMap == null)
            return null

        if (termMap["category"] == null) {
            if (termMap["subClassOf"] != null) {
                List superClasses = (List) termMap["subClassOf"]

                for (superClass in superClasses) {
                    if (superClass == null || superClass["@id"] == null) {
                        continue
                    }
                    String superClassType = jsonld.toTermKey( (String) superClass["@id"] )
                    String category = getMarcCollectionInHierarchy(superClassType, jsonld)
                    if ( category != null )
                        return category
                }
            }
            return null
        }
        else {
            String marcCategory = getMarcCollectionForTerm(termMap)
            if (marcCategory != null) {
                return marcCategory
            }
        }
    }

    static String getMarcCollectionForTerm(Map termMap) {
        def categories = termMap["category"]
        if (!(categories instanceof List)) {
            categories = categories ? [categories] : []
        }
        for (category in categories) {
            String id = category["@id"]
            String collection = MARC_COLLECTION_BY_CATEGORY[id]
            if (collection != null) {
                return collection
            }
        }
        return null
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
