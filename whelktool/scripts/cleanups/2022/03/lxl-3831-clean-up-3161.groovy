import java.util.concurrent.atomic.AtomicInteger

okReport = getReportWriter("ok.tsv")
notOkReport = getReportWriter("not-ok.tsv")
errorReport = getReportWriter("errors.tsv")

def ids = new URL("http://xlbuild.libris.kb.se/3161-modified.txt").getText().readLines()

ids.parallelStream().forEach(this::process)

class ScriptGlobal {
    static AtomicInteger count = new AtomicInteger()
} 

void process(String id) {
    try {
        Map m = getBeforeAfter3161(id)
        process3161(m.before)
        def getThing = { getAtPath(it, ['@graph', 1]) }
        def ok = getThing(m.before) == getThing(m.after)
        
        def msg = "$id\t${diffLink(id, m.beforeVersion, m.afterVersion)}"
        (ok ? okReport : notOkReport).println()
        if (!ok) {
            println(msg)
        }

        int count = ScriptGlobal.count.incrementAndGet()
        if (count % 100 == 0) {
            println(count)
        }
    }
    catch (Exception e) {
        errorReport.println("${id}\t${e}")
    }
}

String diffLink(id, v1, v2) {
    "http://xlbuild.libris.kb.se/tmp/niklin/diffview/?a=https%3A%2F%2Flibris.kb.se%2F$id%2Fdata.jsonld%3Fversion%3D$v1&b=https%3A%2F%2Flibris.kb.se%2F$id%2Fdata.jsonld%3Fversion%3D$v2"
}

Map getBeforeAfter3161(String id) {
    def SCRIPT_3161 = "https://libris.kb.se/sys/globalchanges/2020/05/lxl-3161-move-linkfield-to-instance/script.groovy"
    def version=-1
    while(true) {
        def doc = getDoc(id, version--)
        if (getAtPath(doc, ['@graph',0, 'generationProcess', '@id']) == SCRIPT_3161) {
            return [
                    beforeVersion: version,
                    afterVersion: version + 1,
                    before: getDoc(id, version),
                    after: doc,
            ]
        }
    }
}

static Map getDoc(String id, int version) {
    def json = new URL("https://libris.kb.se/$id?embellished=false&version=$version")
            .getText(requestProperties: ['Accept': 'application/ld+json'])
    new groovy.json.JsonSlurper().parseText(json)
}

// ---------------------------------------------------------------------------------------------------

static Object getAtPath(item, Iterable path, defaultTo = null) {
    if(!item) {
        return defaultTo
    }

    for (int i = 0 ; i < path.size(); i++) {
        def p = path[i]
        if (p == '*' && item instanceof Collection) {
            return item.collect { getAtPath(it, path.drop(i + 1), []) }.flatten()
        }
        else if (item[p] != null) {
            item = item[p]
        } else {
            return defaultTo
        }
    }
    return item
}

// ---------------------------------------------------------------------------------------------------
// Slightly edited version of LXL-3161 script

/*
 * Move properties from Work to Instance and make object an Instance
 *
 * See LXL-3161
 *
 */


failedIDs = getReportWriter("failed-to-update")
scheduledForChange = getReportWriter("scheduled-for-change")
deviantRecords = getReportWriter("deviant-records-to-analyze")


void process3161(Map doc) {
    def LINK_FIELDS_WORK = ['translationOf', 'translation', 'supplement', 'supplementTo', 'hasPart',
                            'continues', 'continuesInPart', 'precededBy', 'precededInPartBy',
                            'mergerOf', 'absorbed', 'absorbedInPart', 'separatedFrom', 'continuedBy',
                            'continuedInPartBy', 'succeededBy', 'succeededInPartBy', 'absorbedBy',
                            'absorbedInPartBy', 'splitInto', 'mergedToForm', 'relatedTo' ]
    def HAS_PART = 'hasPart'
    
    boolean changed = false
    def (record, thing, potentialWork) = doc.'@graph'
    def work = getWork(thing, potentialWork)

    if (work == null) {
        failedIDs.println("Failed to process ${record[ID]} due to missing work entity")
        return
    }

    work.subMap(LINK_FIELDS_WORK).each { key, val ->
        List newListOfObjects = moveToInstance(key, val, record[ID])

        if (!newListOfObjects.isEmpty()) {
            if (thing.containsKey(key))
                thing[key].addAll(newListOfObjects)
            else
                thing[key] = newListOfObjects
            changed = true
        }
    }
    //If empty after removing legacy properties, remove property
    work.entrySet().removeIf { (LINK_FIELDS_WORK.contains(it.key) || it.key == HAS_PART) &&
            it.value.size() == 0 }
}

List moveToInstance(property, listOfWorkObjects, docID) {
    List newListOfObjects = []
    Iterator iter = listOfWorkObjects.iterator()

    while (iter.hasNext()) {
        Object workEntity = iter.next()
        def instanceObject = remodelObjectToInstance(property, workEntity, docID)
        if (instanceObject) {
            newListOfObjects << instanceObject
            iter.remove()
        }
    }

    return newListOfObjects
}

Map remodelObjectToInstance(property, object, docID) {
    def TRANSLATION_OF = 'translationOf'
    def HAS_INSTANCE = 'hasInstance'
    def DISPLAY_TEXT = 'marc:displayText'
    def HAS_PART = 'hasPart'
    
    Map instanceProperties = [(TYPE):'Instance']
    Map workProperties = [(TYPE):'Work']
    Map newInstanceObject = [:]
    boolean isLinked = false

    if (object.hasInstance && object.hasInstance instanceof List && object.hasInstance?.size() > 1) {
        deviantRecords.println "${docID} contains more than one hasInstance entity"
        return
    }

    if ((property == HAS_PART || property == TRANSLATION_OF) &&
            !(object.containsKey(HAS_INSTANCE) || object.containsKey(DISPLAY_TEXT))) {
        return
    }

    object.each {
        //Check if object already is linked
        if (it.key == 'hasInstance' && it.value instanceof List && it.value.any { it.containsKey(ID) }) {
            isLinked = true
            newInstanceObject = it.value.find { it[ID] }
        } else if (it.key == ID){
            isLinked = true
            newInstanceObject << it
        }

        if (!isLinked) {
            if (it.key == 'hasInstance') {
                instanceProperties << it.value
            } else if (it.key == DISPLAY_TEXT) {
                instanceProperties << it
            } else {
                workProperties << it
            }
        }
    }

    //Move qualifier from Work.hasTitle to Instance.hasTitle
    if (workProperties.containsKey('hasTitle')) {

        if (workProperties['hasTitle'].size() > 1) {
            deviantRecords.println "${docID} contains more than one work hasTitle entity"
        } else {
            def qualifier = getAndRemoveQualifierFromWork(workProperties['hasTitle'])
            if (qualifier) {
                if (!instanceProperties.containsKey('hasTitle')) {
                    instanceProperties['hasTitle'] = [[(TYPE): 'Title']]
                }
                instanceProperties['hasTitle'][0].put('qualifier', qualifier)
                if ((workProperties['hasTitle'][0].keySet() - TYPE).size() == 0) {
                    workProperties.remove('hasTitle')
                }
            }
        }
    }

    if ((workProperties.keySet() - TYPE).size() > 0) {
        instanceProperties << ['instanceOf': workProperties]
    }

    if ((instanceProperties.keySet() - TYPE).size() > 0) {
        newInstanceObject << instanceProperties
    }

    return newInstanceObject
}

String getAndRemoveQualifierFromWork(titleEntity) {
    def qualifier
    titleEntity.each {
        if (it.containsKey('qualifier')) {
            qualifier = it['qualifier']
            it.remove('qualifier')
        }
    }
    return qualifier
}

Map getWork(thing, work) {
    if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    } else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}