/*
 Link up local entities with "prefLabel", where a concept exists that have
 the label in question as either hasVariant -> prefLabel or altLabel.
 */

import java.util.concurrent.ConcurrentHashMap

PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")
PrintWriter failedUpdating = getReportWriter("failed-updates")
PrintWriter collisions = getReportWriter("preflabel-collisions")

Map<String, String> termToUri = new ConcurrentHashMap<>()
Set<String> collidingLabels = Collections.newSetFromMap(new ConcurrentHashMap<>())

selectBySqlWhere("collection = 'auth'") { data ->
    Map mainEntity = data.graph[1]

    if (mainEntity.hasVariant != null) {
        asList(mainEntity.hasVariant).each { variant ->
            if (variant["prefLabel"] != null) {
                asList(variant["prefLabel"]).each { prefLabel ->
                    if (termToUri[prefLabel] != null) {
                        collisions.println(prefLabel + " collision, used by both " +
                                termToUri[prefLabel] + " and "  + mainEntity["@id"])
                        collidingLabels.add(prefLabel)
                    }
                    else
                        termToUri.put(prefLabel, mainEntity["@id"])
                }
            }
        }
    }

    if (mainEntity.altLabel != null) {
        asList(mainEntity.altLabel).each { label ->
            if (termToUri[label] != null) {
                collisions.println(label + " collision, used by both " +
                        termToUri[label] + " and "  + mainEntity["@id"])
                collidingLabels.add(label)
            }
            else
                termToUri.put(label, mainEntity["@id"])
        }
    }

    // Map up proper term labels too, we'll link up "everyting we can"
    if (mainEntity.prefLabel != null) {
        asList(mainEntity.prefLabel).each { label ->
            termToUri.put(label, mainEntity["@id"])
        }
    }

    collidingLabels.each { term ->
        termToUri.remove(term)
    }
}

// Link up terms where possible
selectByCollection("bib") { data ->
    boolean changed = traverse(data.graph, termToUri)

    if (changed) {
        scheduledForUpdating.println("${data.doc.getURI()}")
        data.scheduleSave(onError: { e ->
            failedUpdating.println("Failed to update ${data.doc.shortId} due to: $e")
        })
    }
}

boolean traverse(Object node, Map termToUri) {
    boolean changed = false

    if (node instanceof Map) {

        if (node["@id"] == null && node["prefLabel"] != null && termToUri[node["prefLabel"]] != null) {

            String newUri = termToUri[node["prefLabel"]]
            System.out.println("Changing:\n" + node + "\nto:" + ["@id" : newUri] + "\n")
            node.clear()
            node["@id"] = newUri
            changed = true
        }

        for (Object key : node.keySet()) {
            changed |= traverse(node[key], termToUri)
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
