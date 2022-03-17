/*
Fix records mangled by the script for LXL-3161.

Problem:
The variable on this line [1] becomes a script global variable because it is missing a type or def.
It is shared by all selectBy... worker threads and some percentage of records are mangled. 
(TODO: remove this footgun caused by horrible scoping rules of Groovy scripts)

[1] https://github.com/libris/librisxl/blob/a05cc9d35a38c1180fd92ef67e0da9c3a5c123a4/whelktool/scripts/2020/05/lxl-3161-move-linkfield-to-instance/script.groovy#L64

Finding bad records:
- For all modified ids from whelktool report (2689270 records) 
 - Find the versions before and after LXL-3161 was run
 - Run LXL-3161 (with the bug fixed) on the 'before version'
 - If we get the a different result than the 'after version' something is wrong
 
Fixing bad records:
- If the 'after version' and 'current version' are the same (for all properties that might be affected) no one 
  has touched the record since and we can fix it easily. 
- Since the bug only affects the data _within_ a property we can just overwrite all properties with the result from 
  our corrected version of LXL-3161
*/

import java.util.concurrent.atomic.AtomicInteger

class ScriptGlobal {
    static AtomicInteger badCount = new AtomicInteger()
    static AtomicInteger count = new AtomicInteger()

    // Copied from LXL-3161 script below
    static Set LINK_FIELDS_WORK = ['translationOf', 'translation', 'supplement', 'supplementTo', 'hasPart',
                                   'continues', 'continuesInPart', 'precededBy', 'precededInPartBy',
                                   'mergerOf', 'absorbed', 'absorbedInPart', 'separatedFrom', 'continuedBy',
                                   'continuedInPartBy', 'succeededBy', 'succeededInPartBy', 'absorbedBy',
                                   'absorbedInPartBy', 'splitInto', 'mergedToForm', 'relatedTo' ]
}

notOkReport = getReportWriter("not-ok.tsv")
notFixableReport = getReportWriter("not-fixable.tsv")
errorReport = getReportWriter("errors.tsv")

whelk = getWhelk()

def ids = new URL("http://xlbuild.libris.kb.se/3161-modified.txt").getText().readLines()

//ids.parallelStream().forEach(this::process)
ids.stream().forEach(this::process)

void process(String id) {
    try {
        Map m = getBeforeAfter3161(id)
        process3161(m.before) // modified in place
        Map correct = m.before
        def ok = getThing(correct) == getThing(m.after)

        int count = ScriptGlobal.count.incrementAndGet()
        if (count % 100 == 0) { 
            println(count) 
        }
                
        if (!ok) {
            def msg = "$id\t${diffLink(id, m.beforeVersion, m.afterVersion)}"
            notOkReport.println(msg)
            
            println("${ScriptGlobal.badCount.incrementAndGet()} / $count $msg")
            
            Map brokenThing = getThing(m.after)
            def affectedProps = brokenThing.keySet().intersect(ScriptGlobal.LINK_FIELDS_WORK)
            
            Map correctThing = getThing(correct) 
            def badProps = affectedProps.findAll{ brokenThing[it] != correctThing[it] }
            
            Map currentThing = getThing(getDoc(id))
            
            def fixable = brokenThing.subMap(badProps) == currentThing.subMap(badProps)
            if (fixable) {
                overWriteThingProps(id, correctThing.subMap(badProps))
            }
            else {
                notFixableReport.println("$id\t${diffLink(id, m.afterVersion, -1)}")
            }
        }
    }
    catch (Exception e) {
        errorReport.println("${id}\t${e}")
    }
}

String diffLink(id, v1, v2) {
    "http://xlbuild.libris.kb.se/tmp/niklin/diffview/?a=https%3A%2F%2Flibris.kb.se%2F$id%2Fdata.jsonld%3Fversion%3D$v1&b=https%3A%2F%2Flibris.kb.se%2F$id%2Fdata.jsonld%3Fversion%3D$v2"
}

Map getThing(Map doc) { 
    getAtPath(doc, ['@graph', 1]) 
}

Map getBeforeAfter3161(String id) {
    def SCRIPT_3161 = "https://libris.kb.se/sys/globalchanges/2020/05/lxl-3161-move-linkfield-to-instance/script.groovy"
    def version= 1
    while(true) {

        def doc = getDoc(id, version)
        if (getAtPath(doc, ['@graph',0, 'generationProcess', '@id']) == SCRIPT_3161) {
            return [
                    beforeVersion: version - 1,
                    afterVersion: version,
                    before: getDoc(id, version - 1),
                    after: doc,
            ]
        }
        version++
    }
}

Map getDoc(String id, int version = -1) {
    def doc = whelk.storage.load(id, "$version")
    if (doc == null) {
        throw new RuntimeException("Not found. id: $id, version: $version")
    }
    if (doc.deleted) {
        throw new RuntimeException("Deleted id: $id, version: $version")
    }
    return doc.data

}

private void overWriteThingProps(String id, Map properties) {
    selectByIds([id]) { bib -> 
        def(record, thing) = bib.graph
        thing.putAll(properties)
        bib.scheduleSave()
    }
}

def getWhelk() {
    def whelk = null
    selectByIds(['https://id.kb.se/marc']) { docItem ->
        whelk = docItem.whelk
    }
    if (!whelk) {
        throw new RuntimeException("Could not get Whelk")
    }
    return whelk
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
// Everything below is a slightly edited version of LXL-3161 script
// - So that we can pass the doc instead of the script fetching it from the DB
// - Fixed the bug so that it always makes correct changes  

/*
 * Move properties from Work to Instance and make object an Instance
 *
 * See LXL-3161
 *the
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