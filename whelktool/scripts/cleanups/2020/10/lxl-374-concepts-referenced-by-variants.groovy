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
    boolean juvenile = false
    if (inScheme.contains("barn")) {
        juvenile = true
    }
    Set keys = [
            inScheme + "|" + type + "|" + juvenile + "|" +  label, // both inScheme and @type
            inScheme + "||" + juvenile + "|" +  label, // just inScheme
            "|" + type + "|" + juvenile + "|" +  label, // just @type
            "||" + juvenile + "|" +  label, // neither
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
    boolean juvenile = false
    def mainEntity = data.graph[1]
    if (mainEntity["instanceOf"])
        if (mainEntity["instanceOf"]["intendedAudience"])
            if (mainEntity["instanceOf"]["intendedAudience"]["@id"] == "https://id.kb.se/marc/Juvenile")
                juvenile = true
    boolean changed = traverse(data.graph, State.termToUri, juvenile)

    /*if (changed) {
        State.scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            State.failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }*/
}

boolean traverse(Object node, Map termToUri, boolean juvenile) {
    boolean changed = false

    if (node instanceof Map) {

        String inScheme = null
        if (node["inScheme"]) {
            inScheme = node["inScheme"]["@id"]
            if (inScheme == null) {
                // A scheme without @id, don't match against scheme-less concepts.
                inScheme = "_non_authorized" // anything but null or empty string
            }
        }
        if (inScheme == null)
            inScheme = ""

        String prefLabel = node["prefLabel"] ? node["prefLabel"] : ""
        String type = node["@type"] ? node["@type"] : ""

        List keys = [ inScheme + "|" + type + "|" + juvenile + "|" + prefLabel ]
        if (juvenile) // It's ok for children's books to link normal SAO, but not the other way around.
            keys.add( inScheme + "|" + type + "|" + false + "|" + prefLabel )

        for (String key : keys) {
            if (node["@id"] == null && node["prefLabel"] != null && termToUri[key] != null) {

                String newUri = termToUri[key]
                System.out.println("Changing:\n" + node + "\nto:" + ["@id" : newUri] + "\n")
                node.clear()
                node["@id"] = newUri
                changed = true
            }
        }

        // Keep scanning
        for (Object k : node.keySet()) {
            changed |= traverse(node[k], termToUri, juvenile)
        }
    }

    else if (node instanceof List) {
        for (int i = 0; i < node.size(); ++i) {
            changed |= traverse(node[i], termToUri, juvenile)
        }
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
