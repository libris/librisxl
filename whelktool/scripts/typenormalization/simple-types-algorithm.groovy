import whelk.JsonLd
import whelk.Whelk
import whelk.converter.BibTypeNormalizer

import static whelk.JsonLd.ID_KEY as ID

def missingCategoryLog = getReportWriter("missing_category_log.tsv")
def errorLog = getReportWriter("error_log.txt")


// ----- Main action -----

// NOTE: Since instance and work types may co-depend; fetch work and normalize
// that in tandem. We store work ids in memory to avoid converting again.
// TODO: Instead, normalize linked works first, then instances w/o linked works?
convertedWorks = java.util.concurrent.ConcurrentHashMap.newKeySet()

typeNormalizer = new BibTypeNormalizer(getWhelk().resourceCache)

boolean normalizeAndCheck(BibTypeNormalizer typeNormalizer, Map instance, Map work, def missingCategoryLog) {
    var oldItype = typeNormalizer.getType(instance)
    var oldWtype = typeNormalizer.getType(work)

    var changed = typeNormalizer.normalize(instance, work)

    if (!instance.category || instance.category.isEmpty()){
        missingCategoryLog.println("${instance[ID]}\tNo INSTANCE categories after reducing work / instance types\t${oldWtype} / ${oldItype}")
    }

    if (!work.category || work.category.isEmpty()){
        missingCategoryLog.println("${instance[ID]}\tNo WORK categories after reducing work / instance types\t${oldWtype} / ${oldItype}")
    }

    return changed
}


process { def doc, Closure loadWorkItem ->
    def (record, mainEntity) = doc.graph

    try {
        // If mainEntity contains "instanceOf", it's an instance
        if ('instanceOf' in mainEntity) {
            // Instances and locally embedded works
            if (ID !in mainEntity.instanceOf) {
                def work = mainEntity.instanceOf
                if (work instanceof List && work.size() == 1) {
                    work = work[0]
                }

                var changed = normalizeAndCheck(typeNormalizer, mainEntity, work, missingCategoryLog)

                if (changed) doc.scheduleSave()

            } else {
                // Instances and linked works
                def loadedWorkId = mainEntity.instanceOf[ID]
                // TODO: refactor very hacky solution...
                loadWorkItem(loadedWorkId) { workIt ->
                    def (workRecord, work) = workIt.graph

                    var changed = normalizeAndCheck(typeNormalizer, mainEntity, work, missingCategoryLog)

                    if (changed) {
                        doc.scheduleSave()
                        if (loadedWorkId !in convertedWorks) workIt.scheduleSave()
                    }
                    convertedWorks << loadedWorkId
                }
            }
        } else if ('hasInstance' in mainEntity) {
            // Else if it contains the property 'hasInstance', it's a Signe work that reuqires special handling
            var changed = typeNormalizer.normalize([:], mainEntity)
            if (changed) {
                if (mainEntity[ID] !in convertedWorks) doc.scheduleSave()
            }
            convertedWorks << mainEntity[ID]
        }
    }
    catch(Exception e) {
        errorLog.println("${mainEntity[ID]} $e")
        e.printStackTrace(errorLog)
        e.printStackTrace()
    }
}
