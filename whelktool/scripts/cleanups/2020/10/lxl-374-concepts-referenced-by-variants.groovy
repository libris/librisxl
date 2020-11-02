/*
 Link up local entities with "prefLabel", where a concept exists that have
 the label in question as either prefLabel, hasVariant -> prefLabel or altLabel.
 */


/* PROB 1
[@type:Topic, inScheme:[code:albt//swe, @type:ConceptScheme, sameAs:[[@id:https://id.kb.se/term/albt%2F%2Fswe]]], prefLabel:Europa]
to:[@id:http://kblocalhost.kb.se:5000/rp354mt934f3fm1#it]
 */

// PROB 2, Subdivision

import java.util.concurrent.ConcurrentHashMap

class State
{
    static PrintWriter scheduledForUpdating
    static PrintWriter failedUpdating
    static PrintWriter collisions
    static Map<String, String> termToUri
    static Set<String> collidingLabels
}
State.scheduledForUpdating = getReportWriter("scheduled-updates")
State.failedUpdating = getReportWriter("failed-updates")
State.collisions = getReportWriter("preflabel-collisions")
State.termToUri = new ConcurrentHashMap<>()
State.collidingLabels = Collections.newSetFromMap(new ConcurrentHashMap<>())

selectBySqlWhere("collection = 'auth'") { data ->
    Map mainEntity = data.graph[1]

    if (mainEntity.hasVariant != null) {
        asList(mainEntity.hasVariant).each { variant ->
            if (variant["prefLabel"] != null) {
                asList(variant["prefLabel"]).each { prefLabel ->
                    String inScheme = mainEntity["inScheme"] ? mainEntity["inScheme"]["@id"] : null
                    addToTermUriMap(prefLabel, mainEntity["@id"], inScheme)
                }
            }
        }
    }

    if (mainEntity.altLabel != null) {
        asList(mainEntity.altLabel).each { label ->
            String inScheme = mainEntity["inScheme"] ? mainEntity["inScheme"]["@id"] : null
            addToTermUriMap(label, mainEntity["@id"], inScheme)
        }
    }

    // Map up proper term labels too, we'll link up "everyting we can"
    if (mainEntity.prefLabel != null) {
        asList(mainEntity.prefLabel).each { label ->
            String inScheme = mainEntity["inScheme"] ? mainEntity["inScheme"]["@id"] : null
            addToTermUriMap(label, mainEntity["@id"], inScheme)
        }
    }

    State.collidingLabels.each { term ->
        State.termToUri.remove(term)
    }
}

void addToTermUriMap(String label, String uri, String inScheme) {

    List keys = [ inScheme + "|" + label, label ]

    for (String key : keys) {
        if (State.termToUri[key] != null) {
            if (inScheme == null) {
                State.collisions.println("(unqualified) " + label + " collision, used by both " +
                        State.termToUri[key] + " and "  + uri)
            } else {
                State.collisions.println("(inScheme: " + inScheme + ") " + label + " collision, used by both " +
                        State.termToUri[key] + " and "  + uri)
            }
            State.collidingLabels.add(key)
        }
        else {
            State.termToUri.put(key, uri)
        }
    }
}

// Link up terms where possible
selectByCollection("bib") { data ->
    boolean changed = traverse(data.graph, State.termToUri)

    /*
    if (changed) {
        State.scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            State.failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }*/
}

boolean traverse(Object node, Map termToUri) {
    boolean changed = false

    if (node instanceof Map) {

        String inScheme = null
        if (node["inScheme"])
            inScheme = node["inScheme"]["@id"]
        String prefLabel = node["prefLabel"]
        String key = prefLabel
        if (inScheme != null)
            key = inScheme + "|" + prefLabel

        if (node["@id"] == null && node["prefLabel"] != null && termToUri[key] != null) {

            String newUri = termToUri[key]
            System.out.println("Changing:\n" + node + "\nto:" + ["@id" : newUri] + "\n")
            node.clear()
            node["@id"] = newUri
            changed = true
        }

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

private List asList(Object o) {
    if (o == null)
        return []
    if (o instanceof List)
        return o
    return [o]
}
