/**
 For @type Topic: Move hasVariant.prefLabel to altLabel

 Existing data has three basic shapes in hasVariant
    1) ['@type': 'Topic', 'prefLabel': "..."]
    2) ['@type': 'Topic', 'prefLabel': "...", 'marc:controlSubfield': ...]
    3) ['@type': 'ComplexSubject', 'prefLabel': "...", 'termComponentList': ...]

 For now, we only handle the case where all hasVariant have shape #1. The others are TBD.

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
    catch(IllegalArgumentException e) {
        Script.errors.println("${auth.doc.shortId} ${e.getMessage()}")
    }
    catch(Exception e) {
        Script.errors.println("${auth.doc.shortId} $e")
        e.printStackTrace(Script.errors)
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

    asList(thing['hasVariant']).each { Map variant ->
        checkVariantShape(variant)
        altLabel.add(variant['prefLabel'])
    }

    thing['altLabel'] = altLabel.toSorted()
    thing.remove('hasVariant')

    Script.modified.println("${auth.doc.shortId} ${thing['altLabel']}")
    auth.scheduleSave()
}

List asList(o) {
    (o instanceof List) ? (List) o : (o ? [o] : [])
}

void checkVariantShape(Map variant) {
    if (variant['@type'] != 'Topic' || !variant['prefLabel'] || variant.size() != 2) {
        throw new IllegalArgumentException("Unhandled shape: $variant")
    }
}