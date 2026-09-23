package whelk.util;

import se.kb.libris.util.marc.Controlfield;
import se.kb.libris.util.marc.Datafield;
import se.kb.libris.util.marc.MarcRecord;
import se.kb.libris.util.marc.Subfield;
import se.kb.libris.util.marc.impl.ControlfieldImpl;
import se.kb.libris.util.marc.impl.DatafieldImpl;
import se.kb.libris.util.marc.impl.SubfieldImpl;

import whelk.Document;
import whelk.JsonLd;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class LegacyIntegrationTools {
    public static final String NO_MARC_COLLECTION = "none";
    public static final String UNDEFINED_MARC_COLLECTION = "undefined";

    // FIXME: de-KBV/Libris-ify
    public static final Map<String, String> MARC_COLLECTION_BY_CATEGORY = Map.of(
            "https://id.kb.se/marc/auth", "auth",
            "https://id.kb.se/marc/bib", "bib",
            "https://id.kb.se/marc/hold", "hold",
            "https://id.kb.se/marc/none", NO_MARC_COLLECTION
    );

    // FIXME: de-KBV/Libris-ify
    public static final String BASE_LIBRARY_URI = "https://libris.kb.se/library/";

    public static String legacySigelToUri(String sigel) {
        if (sigel.startsWith(BASE_LIBRARY_URI))
            return sigel;
        return BASE_LIBRARY_URI + URLEncoder.encode(sigel, StandardCharsets.UTF_8);
    }

    public static String uriToLegacySigel(String uri) {
        if (uri.startsWith(BASE_LIBRARY_URI))
            return URLDecoder.decode(uri.substring(BASE_LIBRARY_URI.length()), StandardCharsets.UTF_8);
        return null;
    }

    /**
     * Will return "auth", "bib", "hold", "definitions" or "none"
     */
    public static String determineLegacyCollection(Document document, JsonLd jsonld) {
        String type = document.getThingType(); // for example "Instance"

        return getMarcCollectionInHierarchy(type, jsonld);
    }

    public static String getMarcCollectionInHierarchy(String type, JsonLd jsonld) {
        String collection = _getMarcCollectionInHierarchy(type, jsonld);
        return collection.equals(UNDEFINED_MARC_COLLECTION) ? NO_MARC_COLLECTION : collection;
    }

    public static String _getMarcCollectionInHierarchy(String type, JsonLd jsonld) {
        Map<String, Object> termMap = jsonld.vocabIndex.get(type);
        if (termMap == null)
            return UNDEFINED_MARC_COLLECTION;

        String marcCategory = getMarcCollectionForTerm(termMap);
        if (!marcCategory.equals(UNDEFINED_MARC_COLLECTION)) {
            return marcCategory;
        }

        List<?> superClasses = (List<?>) termMap.get("subClassOf");
        if (superClasses == null) {
            return UNDEFINED_MARC_COLLECTION;
        }

        for (Object superClass : superClasses) {
            if (!(superClass instanceof Map<?, ?> superClassMap) || superClassMap.get("@id") == null) {
                continue;
            }
            String superClassType = jsonld.toTermKey(superClassMap.get("@id").toString());
            String category = _getMarcCollectionInHierarchy(superClassType, jsonld);
            if (!category.equals(UNDEFINED_MARC_COLLECTION))
                return category;
        }

        return UNDEFINED_MARC_COLLECTION;
    }

    public static String getMarcCollectionForTerm(Map<String, Object> termMap) {
        Object categoriesValue = termMap.get("category");
        List<?> categories = categoriesValue instanceof List<?> list
                ? list
                : categoriesValue != null ? List.of(categoriesValue) : List.of();
        for (Object category : categories) {
            Object id = ((Map<?, ?>) category).get("@id");
            String collection = id != null ? MARC_COLLECTION_BY_CATEGORY.get(id.toString()) : null;
            if (collection != null) {
                return collection;
            }
        }
        return UNDEFINED_MARC_COLLECTION;
    }

    /**
     * Tomcat incorrectly strips away double slashes from the pathinfo. Compensate here.
     */
    public static String fixUri(String uri) {
        if (uri.matches("/http:/[^/].+")) {
            uri = uri.replace("http:/", "http://");
        } else if (uri.matches("/https:/[^/].+")) {
            uri = uri.replace("https:/", "https://");
        }
        return uri;
    }

    // FIXME: de-KBV/Libris-ify
    /**
     * Take a MARC record from another system, and make it a LIBRIS MARC record.
     *
     * After calling this on a record, you SHOULD IMMEDIATELY also set a new 001 on that record.
     */
    public static void makeRecordLibrisResident(MarcRecord record) {
        // Add new 035$a
        Controlfield field003 = firstOrNull(record.getControlfields("003")); // non-repeatable
        Controlfield field001 = firstOrNull(record.getControlfields("001")); // non-repeatable

        if (field001 != null && field003 != null && field001.getData()
                != null && field003.getData() != null && !field003.getData()
                .equals("SE-LIBR") && !field003.getData().equals("LIBRIS")) {

            String idInOtherSystem = "(" + field003.getData().trim() + ")" + field001.getData().trim();

            boolean hasRelevant035aAlready = false;
            for (Datafield f : record.getDatafields("035")) {
                for (Subfield sf : f.getSubfields("a")) {
                    if (idInOtherSystem.equals(sf.getData()))
                        hasRelevant035aAlready = true;
                }
            }

            if (!hasRelevant035aAlready) {
                Datafield field035 = new DatafieldImpl("035");
                Subfield a = new SubfieldImpl("a".charAt(0), idInOtherSystem);
                field035.addSubfield(a);
                record.addField(field035);
            }
        }

        // Replace 003
        while (record.getControlfields("003").size() > 0)
            record.getFields().remove(record.getControlfields("003").get(0));

        record.addField(new ControlfieldImpl("003", "SE-LIBR"));
    }

    private static Controlfield firstOrNull(List<? extends Controlfield> fields) {
        return fields.isEmpty() ? null : fields.get(0);
    }
}
