package se.kb.libris

import whelk.Document
import whelk.JsonLd

class SignificantChangeCalculator {

    /**
     * Compares two versions of a document, and mutates postupdateDoc with added
     * ChangeNotes where applicable.
     */
    public static boolean markSignificantChanges(Document preUpdateDoc, Document postUpdateDoc, Date modTime, JsonLd jsonld) {
        List<String> markersToAdd = []

        if (significallyChangedAgent(preUpdateDoc, postUpdateDoc, jsonld))
            markersToAdd.add("https://libris.kb.se/change/agent")

        // Add additional rules..

        if (!postUpdateDoc.data["technicalNote"] || ! postUpdateDoc.data["technicalNote"] instanceof List)
            postUpdateDoc.data["technicalNote"] = []
        for (String marker : markersToAdd) {
            List techNotes = postUpdateDoc.data["technicalNote"]
            techNotes.add(
                    [
                            "@type": "ChangeNote",
                            "category": ["@id": marker],
                            "date": modTime.toInstant().toString()
                    ]
            )
        }
    }

    private static boolean significallyChangedAgent(Document preUpdateDoc, Document postUpdateDoc, JsonLd jsonld) {
        if ( ! jsonld.isSubClassOf( preUpdateDoc.getThingType(), "Agent") ||
             ! jsonld.isSubClassOf( postUpdateDoc.getThingType(), "Agent"))
            return false

        return preUpdateDoc.data["@graph"][1]["name"] != postUpdateDoc.data["@graph"][1]["name"] ||
               preUpdateDoc.data["@graph"][1]["givenName"] != postUpdateDoc.data["@graph"][1]["givenName"] ||
               preUpdateDoc.data["@graph"][1]["familyName"] != postUpdateDoc.data["@graph"][1]["familyName"] ||
               preUpdateDoc.data["@graph"][1]["lifeSpan"] != postUpdateDoc.data["@graph"][1]["lifeSpan"]
    }
}
