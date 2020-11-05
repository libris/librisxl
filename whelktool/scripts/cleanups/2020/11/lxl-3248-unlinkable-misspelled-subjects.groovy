/*
  Replaces local embedded terms that are misspelled, with their proper form.
  This should be seen as a preparation for lxl-374, which will yield better
  results without these misspelled terms.
 */

import java.util.concurrent.ConcurrentHashMap

class State
{
    static Map<String, String> compactToProperTerm
    static Set<String> collidingKeys
    static PrintWriter scheduledForUpdating
    static PrintWriter failedUpdating
}
State.compactToProperTerm = new ConcurrentHashMap<>()
State.collidingKeys = Collections.newSetFromMap(new ConcurrentHashMap<>())
State.scheduledForUpdating = getReportWriter("scheduled-updates")
State.failedUpdating = getReportWriter("failed-updates")

selectBySqlWhere("collection = 'auth'") { data ->
    Map mainEntity = data.graph[1]

    if (mainEntity.hasVariant != null) {
        asList(mainEntity.hasVariant).each { variant ->
            if (variant["prefLabel"] != null) {
                asList(variant["prefLabel"]).each { prefLabel ->
                    String inScheme = mainEntity["inScheme"] ? mainEntity["inScheme"]["@id"] : null
                    addToTermMap(prefLabel, inScheme)
                }
            }
        }
    }

    if (mainEntity.altLabel != null) {
        asList(mainEntity.altLabel).each { label ->
            String inScheme = mainEntity["inScheme"] ? mainEntity["inScheme"]["@id"] : null
            addToTermMap(label, inScheme)
        }
    }

    if (mainEntity.prefLabel != null) {
        asList(mainEntity.prefLabel).each { label ->
            String inScheme = mainEntity["inScheme"] ? mainEntity["inScheme"]["@id"] : null
            addToTermMap(label, inScheme)
        }
    }
}

for (String key : State.collidingKeys) {
    State.compactToProperTerm.remove(key)
}

String compactKey(String s, String inScheme) {
    while (s.endsWith("."))
        s = s.substring(0, s.length()-1)
    s = s.toLowerCase()
    s = s.replace(" ", "")
    return inScheme + "|" + s
}

void addToTermMap(String label, String inScheme) {

    // Arrange the keys on which we wish to index this particular concept
    if (
    inScheme != "https://id.kb.se/term/saogf" &&
            inScheme != "https://id.kb.se/term/sao" &&
            inScheme != "https://id.kb.se/term/barn" &&
            inScheme != "https://id.kb.se/term/barngf"
    )
        inScheme = ""

    String key = compactKey(label, inScheme)

    if (State.compactToProperTerm[key] != null) {
        State.collidingKeys.add(key)
    }
    State.compactToProperTerm.put(key, label)
}

// Link up terms where possible
selectByCollection("bib") { data ->
    boolean changed = traverse(data.graph, State.compactToProperTerm, null)

    if (changed) {
        State.scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            State.failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

boolean traverse(Object node, Map compactToProperTerm, String inheritedInScheme) {
    boolean changed = false

    if (node instanceof Map) {

        /*
         Some of these entities do not carry their own inScheme markers,
         instead the inScheme sometimes sit on a parent object.
         */
        if (node["inScheme"]) {
            inheritedInScheme = node["inScheme"]["@id"]
        }

        changed |= attemptCorrection(node, compactToProperTerm, inheritedInScheme)

        for (Object k : node.keySet()) {
            changed |= traverse(node[k], compactToProperTerm, inheritedInScheme)
        }
    }

    else if (node instanceof List) {
        for (int i = 0; i < node.size(); ++i) {
            changed |= traverse(node[i], compactToProperTerm, inheritedInScheme)
        }
    }
    return changed
}

private boolean attemptCorrection(Object node, Map compactToProperTerm, String inScheme) {
    boolean changed = false

    // A scheme without @id, or in a scheme we're not allowed to touch?
    if (inScheme == null ||
            (inScheme != "https://id.kb.se/term/saogf" &&
                    inScheme != "https://id.kb.se/term/sao" &&
                    inScheme != "https://id.kb.se/term/barn" &&
                    inScheme != "https://id.kb.se/term/barngf") ) {
        return changed
    }

    String prefLabel = node["prefLabel"] ? node["prefLabel"] : ""
    String key = compactKey(prefLabel, inScheme)


    if (prefLabel) {
        String properTerm = compactToProperTerm[key]
        if (properTerm != null && properTerm != prefLabel) {
            //System.out.println("Want to replace \"" + prefLabel + "\" with \"" + properTerm + "\"")
            node["prefLabel"] = properTerm
            changed = true
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
