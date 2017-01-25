package whelk.tools

import groovy.util.logging.Slf4j
import groovyx.gpars.actor.DefaultActor

import java.nio.file.Paths

/**
 * Created by theodortolstoy on 2017-01-11.
 */
@Slf4j
class StatsMaker extends DefaultActor {
    def counter = 0
    def startTime = System.currentTimeMillis()
    int missingBibFields = 0
    def uncertainFileWriter = new FileWriter(Paths.get("uncertainMatches.tsv") as String)
    def completeFileWriter = new FileWriter(Paths.get("completeMatches.tsv") as String)
    Map resultMap = [possibleMatches: 0, matches: 0, MisMatchesOnA: 0, bibInAukt: 0, auktInBib: 0, doubleDiff: 0, specAndDoc: 0, allAuth: 0, ignoredSetSpecs: 0, MissingBibFields: 0]
    def lacksValidAuthRecords = 0
    def docIsNull = 0

    static void printDiffResultsToFile(FileWriter fileWriter, matches, doc, setSpecs) {
        matches.each { match ->
            fileWriter << "${match.type}" +
                    "\t${match.diff.count { it }}" +
                    "\t${match.spec.field}" +
                    "\t${match.bibField}" +
                    "\t${match.subfieldsInOverlap}" +
                    "\t${match.subfieldsInDiff}" +
                    "\t${match.subfieldsInReversediff}" +
                    "\t${match.reverseDiff.count { it }}" +
                    "\t${match.overlap.count { it }}" +
                    "\t${doc.leader?.substring(5, 6) ?: ''}" +
                    "\t${doc.leader?.substring(6, 7) ?: ''}" +
                    "\t${doc.leader?.substring(7, 8) ?: ''}" +
                    "\t${doc.leader?.substring(17, 18) ?: ''}" +
                    "\t${doc.fields?."008"?.find { it -> it }?.take(2) ?: ''}" +
                    "\t _" +
                    "\t${doc.fields?."040"?.find { it -> it }?.subfields?."a"?.find { it -> it } ?: ''}" +
                    "\t${doc.fields?."040"?.find { it -> it }?.subfields?."d"?.find { it -> it } ?: ''}" +
                    "\t${match.spec.data?.leader?.substring(5, 6) ?: ''}" +
                    "\t${match.spec.data?.leader?.substring(6, 7) ?: ''}" +
                    "\t${match.spec.data?.fields?."008"?.find { it -> it }?.take(2) ?: ''}" +
                    "\t${match.spec.data?.fields?."008"?.find { it -> it }?.substring(9, 10) ?: ''}" +
                    "\t${match.spec.data?.fields?."008"?.find { it -> it }?.substring(33, 34) ?: ''}" +
                    "\t${match.spec.data?.fields?."008"?.find { it -> it }?.substring(39, 40) ?: ''}" +
                    "\t${match.numBibFields}" +
                    "\t${match.numAuthFields}" +
                    "\t${match.partialD}" +
                    "\t${match.bibHas035a}" +
                    "\t${match.bibSet} " +
                    "\t${match.authSet} " +
                    "\t${setSpecs.first()?.bibid} " +
                    "\t${setSpecs.first()?.id} " +
                    "\t${match.bibHas240a}" +
                    "\n"

        }

    }

    @Override
    protected void act() {
        loop {
            react { argument ->
                def doc = argument?.doc
                List allAuthRecords = argument?.authData
                if (doc == null)
                    docIsNull++
                if (!SetSpecMatcher.hasValidAuthRecords(allAuthRecords))
                    lacksValidAuthRecords++
                if (doc != null && SetSpecMatcher.hasValidAuthRecords(allAuthRecords)) {
                    List authRecords = allAuthRecords.findAll { authRecord ->
                        !SetSpecMatcher.ignoredAuthFields.contains(authRecord.field)
                    }

                    //int ignoredAuthRecords = allAuthRecords.findAll { authRecord -> SetSpecMatcher.ignoredAuthFields.contains(authRecord.field) }.size()

                    def groupedAuthRecords = SetSpecMatcher.prepareAuthRecords(authRecords)
                    def groupedBibFields = SetSpecMatcher.prepareBibRecords(doc, SetSpecMatcher.fieldRules)

                    /*
                    Saved for later use
                    def possibleBibFieldsFromSetSpec =
                            SetSpecMatcher.authLinkableFields.findAll { f ->
                                authRecords.collect { a -> a.field }.contains(f.authfield)
                            }.collect { a -> a.bibField }

                    def linkableBibFields =
                            groupedBibFields.collectMany { c ->
                                c.value.collect { it.keySet()[0] }
                            }

                    //def bibFieldsWithoutAuthField = linkableBibFields.findAll { f -> !possibleBibFieldsFromSetSpec.contains(f) }
                    */
                    groupedAuthRecords.each { authorityGroup ->
                        def bibFieldGroup = groupedBibFields.find { field ->
                            field.key == authorityGroup.key
                        }
                        if (!bibFieldGroup?.value) {
                            missingBibFields++
                            // def file = new File("/missingBibFields.tsv")
                            // file << "${authorityGroup?.key}\t ${bibFieldsWithoutAuthField} \t${setSpecs.first()?.bibid} \t${setSpecs.first()?.id} \t  http://libris.kb.se/bib/${setSpecs.first()?.bibid}?vw=full&tab3=marc \t http://libris.kb.se/auth/${setSpecs.first()?.id}\n"
                        } else {
                            //println "authority Group: ${authorityGroup?.key}, AuthFields: ${groupedAuthRecords.count { it.value }} against  ${bibFieldGroup?.key}, Bib Fields: ${bibFieldGroup?.value.count { it }} "
                            authorityGroup.value.each { spec ->
                                // println spec.normalizedSubfields

                                def diffs = SetSpecMatcher.getSetDiffs(spec, bibFieldGroup.value, SetSpecMatcher.fieldRules, doc)

                                def completeMatches = diffs.findAll { match -> match.isMatch }

                                def misMatchesOnA = diffs.findAll { match -> match.hasMisMatchOnA }

                                def uncertainMatches = diffs.findAll { match ->
                                    ((match.hasOnlyDiff || match.hasOnlyReverseDiff || match.hasDoubleDiff)
                                            && !match.hasMisMatchOnA && !match.isMatch)
                                }

                                diffs.findAll { match ->
                                    !match.hasOnlyDiff &&
                                            !match.hasOnlyReverseDiff &&
                                            !match.hasDoubleDiff &&
                                            !match.hasMisMatchOnA &&
                                            !match.isMatch
                                }.each { diff ->
                                    println "miss! Diff: ${diff.inspect()}"
                                }


                                resultMap.possibleMatches += 1

                                resultMap.matches += completeMatches.size()
                                resultMap.MisMatchesOnA += misMatchesOnA.size()
                                resultMap.bibInAukt += uncertainMatches.count { match ->
                                    match.hasOnlyDiff
                                }
                                resultMap.auktInBib += uncertainMatches.count { match ->
                                    match.hasOnlyReverseDiff
                                }
                                resultMap.doubleDiff += uncertainMatches.count { match ->
                                    match.hasDoubleDiff
                                }

                                resultMap.specAndDoc++
                                resultMap.allAuth = allAuthRecords.size()
                                resultMap.ignoredSetSpecs = allAuthRecords.size() - authRecords.size()
                                resultMap.MissingBibFields = missingBibFields

                                if (uncertainMatches.any()) {
                                    printDiffResultsToFile(uncertainFileWriter, uncertainMatches, doc, authRecords)
                                }
                                if (completeMatches.any()) {
                                    printDiffResultsToFile(completeFileWriter, completeMatches, doc, authRecords)

                                }
                            }
                        }
                    }

                    if (++counter % 10000 == 0) {
                        def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                        if (elapsedSecs > 0) {
                            def docsPerSec = counter / elapsedSecs
                            log.info "${counter} ${resultMap}"
                            "Working. Currently ${counter} documents recieved. Crunching ${docsPerSec} docs / s. "

                        }
                    }
                }
                reply true
            }
        }

    }
}
