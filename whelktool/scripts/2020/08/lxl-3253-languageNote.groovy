/**
 * Fix broken subject links.
 *
 * See LXL-3196 for more information.
 *
 * ind1 (dvs marc:languageNote) ska vara ind1=1 om:
 * ett värde i instanceOf,language och ett värde i instanceOf,translationOf,language
 * --> dessa värden är _inte_ desamma.
 * Safe att ta bort marc:languageNote
 * 1. Logga som OK
 *
 * Logga som frågetecken:
 * 2. Logga alla förekomster av språkkod https://id.kb.se/language/zxx
 * 3. Logga alla förekomster av repeterade språk i:
 * instanceOf,translationOf,language
 * instanceOf,language
 *
 * 4. Logga de förekomster som inte täcks upp av ovanstående
 */

import whelk.util.Statistics

class Script {
    static Statistics s = new Statistics().printOnShutdown()
    static PrintWriter ok
    static PrintWriter nonLinguisticContent
    static PrintWriter repeatedLanugages
    static PrintWriter furtherAnalyze
    static PrintWriter scheduledForChange
}

Script.ok = getReportWriter("ok.txt")
Script.nonLinguisticContent = getReportWriter("non-linguistic-content.txt")
Script.repeatedLanugages = getReportWriter("repeated-lanugages.txt")
Script.furtherAnalyze = getReportWriter("further-analyze.txt")
Script.scheduledForChange = getReportWriter("scheduledForChange.txt")

selectBySqlWhere("""
        collection = 'bib' AND (
            data#>>'{@graph,1,instanceOf,translationOf}' IS NOT NULL OR
            data#>>'{@graph,1,instanceOf,language}' IS NOT NULL
        )
        """) { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println("BibId: ${bib.graph[0][ID]} $e")
        e.printStackTrace()
    }
}

void process(bib) {
    Map work = getWork(bib)
    boolean changed = false

    if (!work) {
        return
    }

    if (work.containsKey('language') && work.containsKey('translationOf')) {
        boolean containsDeviantLang = false
        //Uniform data and log deviant language entities
        changed |= uniformToList(work, 'language')
        containsDeviantLang |= isDeviantLanguageEntity(work, bib.graph[0][ID])

        changed |= uniformToList(work, 'translationOf')
        work.translationOf.each {
            changed |= uniformToList(it, 'language')
            containsDeviantLang |= isDeviantLanguageEntity(it, bib.graph[0][ID])
        }

        //Check if translationOf contains several objects containing language. Log and return
        listOfLanguages = work?.translationOf.findResults { it.language }.flatten()

        if (!containsDeviantLang) {
            if (listOfLanguages && listOfLanguages.size() > 1) {
                Script.repeatedLanugages.println("${bib.graph[0][ID]} Repeated in translationOf: ${listOfLanguages}")
            } else if (listOfLanguages && isTranslationOf(work?.language, listOfLanguages[0])) {
                Script.ok.println("${bib.graph[0][ID]} -> language: ${work?.language} + translationOf: ${work?.translationOf}")
            } else {
                Script.furtherAnalyze.println("${bib.graph[0][ID]} -> language: ${work?.language} + translationOf: ${work?.translationOf}")
            }
        }

    }
    if (changed) {
        Script.scheduledForChange.println "Record was updated ${bib.graph[0][ID]}"
        bib.scheduleSave(onError: { e ->
            failedIDs.println("Failed to save ${bib.graph[0][ID]} due to: $e")
        })
    }
}

Map getWork(bib) {
    def (_record, thing, work) = bib.graph
    if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    } else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}

boolean isDeviantLanguageEntity(entity, docId) {
    boolean isDeviant = false
    if (entity['language'] && entity['language'].size() > 1) {
        Script.repeatedLanugages.println("${docId} Repeated language: ${entity['language']}")
        isDeviant = true
    } else if (entity['language'] && isNonLinguisticContent(entity['language'])) {
        Script.nonLinguisticContent.println("${docId}: ${entity['language']}")
        isDeviant = true
    }
    return isDeviant
}

boolean uniformToList(subject, key)  {
    if (subject.containsKey(key) && subject[key] instanceof Map) {
        subject[key] = [subject[key]]
        return true
    }
}

boolean isNonLinguisticContent(listOfEntities) {
    boolean nonCodeExist = false
    listOfEntities.each {
        if (it[ID] && it[ID].substring(it[ID].lastIndexOf('/') + 1) == 'zxx') {
            nonCodeExist |= true
        }
    }
    return nonCodeExist
}

boolean isTranslationOf(obj, lang) {
    if (obj && obj.size() == 1 && obj[0].containsKey(ID) && lang.containsKey(ID)) {
        return obj[0][ID] != lang[ID]
    }
}