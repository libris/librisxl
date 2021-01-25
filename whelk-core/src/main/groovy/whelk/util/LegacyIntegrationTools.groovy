package whelk.util

import groovy.transform.CompileStatic
import se.kb.libris.util.marc.Controlfield
import se.kb.libris.util.marc.Field
import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.Subfield
import se.kb.libris.util.marc.impl.ControlfieldImpl
import se.kb.libris.util.marc.impl.DatafieldImpl
import se.kb.libris.util.marc.impl.SubfieldImpl
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
        return BASE_LIBRARY_URI + URLEncoder.encode(sigel, "UTF-8")
    }

    static String uriToLegacySigel(String uri) {
        if (uri.startsWith(BASE_LIBRARY_URI))
            return URLDecoder.decode(uri.substring(BASE_LIBRARY_URI.length()), "UTF-8")
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

        String marcCategory = getMarcCollectionForTerm(termMap)
        if (marcCategory != null) {
            return marcCategory
        }

        List superClasses = (List) termMap["subClassOf"]
        if (superClasses == null) {
            return null
        }

        for (superClass in superClasses) {
            if (superClass == null || superClass["@id"] == null) {
                continue
            }
            String superClassType = jsonld.toTermKey( (String) superClass["@id"] )
            String category = getMarcCollectionInHierarchy(superClassType, jsonld)
            if ( category != null )
                return category
        }

        return null
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

    /**
     * Take a MARC record from another system, and make it a LIBRIS MARC record.
     *
     * After calling this on a record, you SHOULD IMMEDIATELY also set a new 001 on that record.
     */
    static void makeRecordLibrisResident(MarcRecord record) {
        // Add new 035$a
        Controlfield field003 = (Controlfield) record.getControlfields("003")[0] // non-repeatable
        Controlfield field001 = (Controlfield) record.getControlfields("001")[0] // non-repeatable

        if (field001 != null && field003 != null && field001.getData()
                != null && field003.getData() != null && field003.getData()
                != "SE-LIBR" && field003.getData() != "LIBRIS") {

            String idInOtherSystem = "(" + field003.getData() + ")" + field001.getData()

            boolean hasRelevant035aAlready = false
            record.getDatafields("035").each { f ->
                f.getSubfields("a").each { sf ->
                    if (sf.getData() == idInOtherSystem)
                        hasRelevant035aAlready = true
                }
            }

            if (!hasRelevant035aAlready) {
                Field field035 = new DatafieldImpl("035")
                Subfield a = new SubfieldImpl("a".charAt(0), idInOtherSystem)
                field035.addSubfield(a)
                record.addField(field035)
            }
        }

        // Replace 003
        while (record.getControlfields("003").size() > 0)
            record.getFields().remove(record.getControlfields("003").get(0));

        record.addField(new ControlfieldImpl("003", "SE-LIBR"))
    }
}
