/*
 Link up local entities with "prefLabel", where a concept exists that have
 the label in question as either prefLabel, hasVariant -> prefLabel or altLabel.
 and matches in terms of inScheme/@type (if such are available)
 */

import java.util.concurrent.ConcurrentHashMap

class State
{
    static PrintWriter scheduledForUpdating
    static PrintWriter failedUpdating
    static Map<String, String> termToUri
    static Set<String> collidingKeys
}
State.scheduledForUpdating = getReportWriter("scheduled-updates")
State.failedUpdating = getReportWriter("failed-updates")
State.termToUri = new ConcurrentHashMap<>()
State.collidingKeys = Collections.newSetFromMap(new ConcurrentHashMap<>())

selectBySqlWhere("collection = 'auth'") { data ->
    Map mainEntity = data.graph[1]

    if (mainEntity.hasVariant != null) {
        asList(mainEntity.hasVariant).each { variant ->
            if (variant["prefLabel"] != null) {
                asList(variant["prefLabel"]).each { prefLabel ->
                    String inScheme = mainEntity["inScheme"] ? mainEntity["inScheme"]["@id"] : null
                    addToTermUriMap(prefLabel, mainEntity["@id"], inScheme, mainEntity["@type"])
                }
            }
        }
    }

    if (mainEntity.altLabel != null) {
        asList(mainEntity.altLabel).each { label ->
            String inScheme = mainEntity["inScheme"] ? mainEntity["inScheme"]["@id"] : null
            addToTermUriMap(label, mainEntity["@id"], inScheme, mainEntity["@type"])
        }
    }

    // Map up proper term labels too, we'll link up "everyting we can"
    if (mainEntity.prefLabel != null) {
        asList(mainEntity.prefLabel).each { label ->
            String inScheme = mainEntity["inScheme"] ? mainEntity["inScheme"]["@id"] : null
            addToTermUriMap(label, mainEntity["@id"], inScheme, mainEntity["@type"])
        }
    }

    State.collidingKeys.each { key ->
        State.termToUri.remove(key)
    }
}

void addToTermUriMap(String label, String uri, String inScheme, String type) {

    // Arrange the keys on which we wish to index this particular concept
    if (
            inScheme != "https://id.kb.se/term/saogf" &&
            inScheme != "https://id.kb.se/term/sao" &&
            inScheme != "https://id.kb.se/term/barn" &&
            inScheme != "https://id.kb.se/term/barngf"
    )
        inScheme = ""
    if (type == null)
        type = ""
    Set keys = [
            inScheme + "|" + type + "£" + label, // both
            inScheme + "|£" + label, // just inScheme
            /* Not permitted by MSS (linking up scheme-less local terms to schemes ones with the same preflabel):
            "|" + type + "£" + label, // just type
            "|£" + label, // neither */
    ]

    for (String key : keys) {
        if (State.termToUri[key] != null) {
            State.collidingKeys.add(key)
        }
        State.termToUri.put(key, uri)
    }
}

// Link up terms where possible
selectByCollection("bib") { data ->
    boolean changed = traverse(data.graph, State.termToUri)

    if (changed) {
        State.scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            State.failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

boolean traverse(Object node, Map termToUri) {
    boolean changed = false

    if (node instanceof Map) {

        changed |= attemptLinkUp(node, termToUri)

        for (Object k : node.keySet()) {
            changed |= traverse(node[k], termToUri)
        }
    }

    else if (node instanceof List) {
        for (int i = 0; i < node.size(); ++i) {
            changed |= traverse(node[i], termToUri)
        }
    }
    return changed
}

private boolean attemptLinkUp(Object node, Map termToUri) {
    boolean changed = false
    String inScheme = null

    if (node["inScheme"]) {
        inScheme = node["inScheme"]["@id"]
        temp = inScheme
    }
    // A scheme without @id, or in a scheme we're not allowed to touch?
    if (inScheme == null ||
            (inScheme != "https://id.kb.se/term/saogf" &&
                    inScheme != "https://id.kb.se/term/sao" &&
                    inScheme != "https://id.kb.se/term/barn" &&
                    inScheme != "https://id.kb.se/term/barngf") ) {
        inScheme = "_non_authorized" // anything but null or empty string
    }

    String prefLabel = node["prefLabel"] ? node["prefLabel"] : ""
    String type = node["@type"] ? node["@type"] : ""
    String key = inScheme + "|" + type + "£" + prefLabel

    if (node["@id"] == null && node["prefLabel"] != null && termToUri[key] != null) {

        String newUri = termToUri[key]
        //System.out.println("Changing:\n" + node + "\nto:" + ["@id" : newUri] + "\n")
        node.clear()
        node["@id"] = newUri
        changed = true
    }

    return changed
}

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
