/**
 For @type Topic: Move hasVariant.prefLabel to altLabel

 Existing data has three basic shapes in hasVariant
    1) ['@type': 'Topic', 'prefLabel': "..."]
    2) ['@type': 'Topic', 'prefLabel': "...", 'marc:controlSubfield': ...]
    3) ['@type': 'ComplexSubject', 'prefLabel': "...", 'termComponentList': ...]

 For now, we only handle case #1. The others are TBD.

 See LXL-3389
 */

class Script {
    static PrintWriter modified
    static PrintWriter errors
}

Script.modified = getReportWriter("modified.txt")
Script.errors = getReportWriter("errors.txt")

selectByCollection('auth') { auth ->
    try {
        process(auth)
    }
    catch(Exception e) {
        Script.errors.println("${auth.doc.shortId} $e")
        e.printStackTrace(Script.errors)
        e.printStackTrace()
    }
}

void process(auth) {
    Map thing = auth.graph[1]

    if (thing['@type'] != 'Topic') {
        return
    }

    if (!thing['hasVariant']) {
        return
    }

    def altLabel = asList(thing['altLabel']) as Set

    boolean modified = false
    thing['hasVariant'] = asList(thing['hasVariant']).findResults { Map variant ->
        if (shapeOk(variant)) {
            altLabel.add(variant['prefLabel'])
            modified = true
            return null // remove
        }
        else {
            return variant // keep
        }
    }

    if (modified) {
        if (thing['hasVariant'].isEmpty()) {
            thing.remove('hasVariant')
        }

        thing['altLabel'] = altLabel.toSorted()
        Script.modified.println("${auth.doc.shortId} ${thing['altLabel']}")
        auth.scheduleSave()
    }
}

List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}

boolean shapeOk(Map variant) {
    variant['@type'] == 'Topic' && variant['prefLabel'] && variant.size() == 2
}