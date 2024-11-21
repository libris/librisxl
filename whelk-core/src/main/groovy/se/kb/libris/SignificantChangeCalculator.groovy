package se.kb.libris

import whelk.Document
import whelk.JsonLd

class SignificantChangeCalculator {

    public static Map getImpliedTechnicalNote(Map note, List typedPath, JsonLd jsonld) {

        // Are conditions met for an implied primary-contribution marker?
        // In other words: Is this (change-) 'note' a '/change/agent' placed on a primaryContribution (or derivative) path?
        if ( typedPathEndsWith(typedPath, ["contribution", "@type=PrimaryContribution", "agent", "@type=Agent", "technicalNote", "@type=ChangeNote"], jsonld) ) {
            if (note["category"] && note["category"] instanceof Map && note["category"]["@id"] && note["category"]["@id"] instanceof String) {
                if (note["category"]["@id"] == "https://libris.kb.se/change/agent") {
                    return [
                            "@type"   : "ChangeNote",
                            "category": ["@id": "https://libris.kb.se/change/primarycontribution"],
                            "date"    : note["date"]
                    ]
                }
            }
        }

        // Check for additional implied markers..

        // Default: Return the original note
        return note
    }

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

    /**
     * Types in 'full' are allowed to inherit types in 'end' and still be considered equal-ending.
     *
     * For example, full = ["instanceOf", "@type=NotatedMusic"] and end = ["instanceOf", "@type=Work"] is considered
     * equal-ending, but if 'full' and 'end' switch places, they are not.
     */
    private static boolean typedPathEndsWith(List<String> full, List<String> end, JsonLd jsonLd) {
        if (end.size() > full.size())
            return false
        List fullEnd = full.subList(full.size() - end.size(), full.size())

        for (int i = 0; i < end.size(); ++i) {
            if (fullEnd[i].startsWith("@type=") && end[i].startsWith("@type=")) {
                String t1 = fullEnd[i].substring("@type=".length())
                String t2 = end[i].substring("@type=".length())
                if (! jsonLd.isSubClassOf(t1, t2) ) {
                    return false
                }
            }
            else if (fullEnd[i] != end[i]) {
                return false
            }
        }
        return true
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
