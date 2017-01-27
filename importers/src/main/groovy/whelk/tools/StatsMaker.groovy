package whelk.tools

import groovy.util.logging.Slf4j
import groovyx.gpars.actor.DefaultActor

import java.nio.file.Paths
import java.sql.Timestamp

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
            react { List<VCopyDataRow> argument ->

                VCopyDataRow row = argument.last()
                def allAuthRecords = PostgresLoadfileWriter.getAuthDocsFromRows(argument)
                Map doc = PostgresLoadfileWriter.getMarcDocMap(row.data)
                if (doc == null)
                    docIsNull++

                if (!SetSpecMatcher.hasValidAuthRecords(allAuthRecords))
                    lacksValidAuthRecords++

                if (doc != null && SetSpecMatcher.hasValidAuthRecords(allAuthRecords)) {

                    def matchResults = SetSpecMatcher.matchAuthToBib(doc, allAuthRecords, true)

                    List authRecords = allAuthRecords.findAll { authRecord ->
                        !SetSpecMatcher.ignoredAuthFields.contains(authRecord.field)
                    }

                    def uncertainMatches = matchResults.findAll { match ->
                        ((match.hasOnlyDiff || match.hasOnlyReverseDiff || match.hasDoubleDiff)
                                && !match.hasMisMatchOnA && !match.isMatch)
                    }

                    matchResults.findAll { match ->
                        !match.hasOnlyDiff &&
                                !match.hasOnlyReverseDiff &&
                                !match.hasDoubleDiff &&
                                !match.hasMisMatchOnA &&
                                !match.isMatch
                    }.each { diff ->
                        println "miss! Diff: ${diff.inspect()}"
                    }


                    def completeMatches = matchResults.findAll { match -> match.isMatch }

                    def misMatchesOnA = matchResults.findAll { match -> match.hasMisMatchOnA }


                    resultMap.possibleMatches += matchResults.count { it }

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
