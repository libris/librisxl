package se.kb.libris

import whelk.Document
import whelk.JsonLd

class SignificantChangeCalculator {

    /**
     * Compares two versions of a document, and mutates postupdateDoc with added
     * ChangeNotes where applicable.
     */
    public static void markSignificantChanges(Document preUpdateDoc, Document postUpdateDoc, Date modTime, JsonLd jsonld) {
        List<String> markersToAdd = []

        if (significantlyChangedAgent(preUpdateDoc, postUpdateDoc, jsonld))
            markersToAdd.add("https://libris.kb.se/change/agent")

        // Add additional rules..

        List newTechNotes = []
        for (String marker : markersToAdd) {
            newTechNotes.add(
                    [
                            "@type": "ChangeNote",
                            "category": ["@id": marker],
                            "date": modTime.toInstant().toString()
                    ]
            )
        }

        if (newTechNotes) {
            if (postUpdateDoc.data["@graph"][1]["technicalNote"] && postUpdateDoc.data["@graph"][1]["technicalNote"] instanceof List) {
                Set notes = postUpdateDoc.data["@graph"][1]["technicalNote"] as Set
                notes.addAll(newTechNotes)
                postUpdateDoc.data["@graph"][1]["technicalNote"] = notes.toList()
            } else {
                postUpdateDoc.data["@graph"][1]["technicalNote"] = newTechNotes
            }
        }
    }

    private static boolean significantlyChangedAgent(Document preUpdateDoc, Document postUpdateDoc, JsonLd jsonld) {
        if ( ! jsonld.isSubClassOf( preUpdateDoc.getThingType(), "Agent") ||
             ! jsonld.isSubClassOf( postUpdateDoc.getThingType(), "Agent"))
            return false

        return preUpdateDoc.data["@graph"][1]["name"] != postUpdateDoc.data["@graph"][1]["name"] ||
               preUpdateDoc.data["@graph"][1]["givenName"] != postUpdateDoc.data["@graph"][1]["givenName"] ||
               preUpdateDoc.data["@graph"][1]["familyName"] != postUpdateDoc.data["@graph"][1]["familyName"] ||
               preUpdateDoc.data["@graph"][1]["lifeSpan"] != postUpdateDoc.data["@graph"][1]["lifeSpan"]
    }
}
