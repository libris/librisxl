package whelk.actors
import whelk.SetSpecMatcher
import whelk.converter.marc.MarcFrameConverter
import whelk.AuthBibMatcher
import whelk.VCopyDataRow
import whelk.importer.MySQLLoader
import whelk.util.ThreadPool

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths

import groovy.util.logging.Slf4j as Log


/**
 * Created by theodortolstoy on 2017-01-11.
 */
@Log
class StatsMaker implements MySQLLoader.LoadHandler {

    ThreadPool threadPool
    int missingBibFields = 0
    BufferedWriter uncertainFileWriter
    BufferedWriter completeFileWriter
    Map resultMap = [possibleMatches : 0,
                     matches         : 0,
                     MisMatchesOnA   : 0,
                     bibInAukt       : 0,
                     auktInBib       : 0,
                     doubleDiff      : 0,
                     specAndDoc      : 0,
                     allAuth         : 0,
                     ignoredAuthRecords : 0,
                     MissingBibFields: 0]

    def lacksValidAuthRecords = 0
    def docIsNull = 0

    StatsMaker() {
        final int THREAD_COUNT = 4 * Runtime.getRuntime().availableProcessors()
        threadPool = new ThreadPool(THREAD_COUNT)
        uncertainFileWriter = Files.newBufferedWriter(
                Paths.get("uncertainMatches.tsv"), Charset.forName("UTF-8"))
        uncertainFileWriter.write(header)
        completeFileWriter = Files.newBufferedWriter(
                Paths.get("completeMatches.tsv"), Charset.forName("UTF-8"))
        completeFileWriter.write(header)

    }


    void handle(List<List<VCopyToWhelkConverter.VCopyDataRow>> batch) {
        threadPool.executeOnThread(batch, { _batch, threadIndex ->

            batch.each { rows ->

                VCopyToWhelkConverter.VCopyDataRow row = rows.last()
                def allAuthRecords = VCopyToWhelkConverter.getAuthDocsFromRows(rows)
                Map doc = VCopyToWhelkConverter.getMarcDocMap(row.data)
                if (doc == null)
                    docIsNull++

                if (!AuthBibMatcher.hasValidAuthRecords(allAuthRecords))
                    lacksValidAuthRecords++

                if (doc != null && AuthBibMatcher.hasValidAuthRecords(allAuthRecords)) {
                    List<Map> matchResults = AuthBibMatcher.matchAuthToBib(doc, allAuthRecords, true)

                    List authRecords = allAuthRecords.findAll {
                        !AuthBibMatcher.ignoredAuthFields.contains(it.field)
                    }

                    def uncertainMatches = matchResults.findAll { Map match ->
                        ((match.hasOnlyDiff || match.hasOnlyReverseDiff || match.hasDoubleDiff)
                                && !match.hasMisMatchOnA && !match.isMatch)
                    }

                    matchResults.findAll { Map match ->
                        !match.hasOnlyDiff &&
                                !match.hasOnlyReverseDiff &&
                                !match.hasDoubleDiff &&
                                !match.hasMisMatchOnA &&
                                !match.isMatch
                    }.each { diff ->
                        log.debug "miss! Diff: ${diff.inspect()}"
                    }


                    def completeMatches = matchResults.findAll { it.isMatch }

                    def misMatchesOnA = matchResults.findAll { it.hasMisMatchOnA }


                    resultMap.possibleMatches += matchResults.count { it }

                    resultMap.matches += completeMatches.size()
                    resultMap.MisMatchesOnA += misMatchesOnA.size()
                    resultMap.bibInAukt += uncertainMatches.count { it.hasOnlyDiff }
                    resultMap.auktInBib += uncertainMatches.count { it.hasOnlyReverseDiff }
                    resultMap.doubleDiff += uncertainMatches.count { it.hasDoubleDiff }

                    resultMap.specAndDoc++
                    resultMap.allAuth = allAuthRecords.size()
                    resultMap.ignoredAuthRecords = allAuthRecords.size() - authRecords.size()
                    resultMap.MissingBibFields = missingBibFields

                    printDiffResultsToFile(uncertainMatches, completeMatches)

                }
            }
        })
    }

    synchronized void printDiffResultsToFile(uncertainMatches, completeMatches) {
        appendtofile(uncertainFileWriter, uncertainMatches)
        appendtofile(completeFileWriter, completeMatches)
    }

    String header = "MatchType" +
            "\tAuthNotInBib" +
            "\tAuktfält" +
            "\tBibfält" +
            "\tÖverensstämmandeDelfält" +
            "\tSubfieldsInDiff" +
            "\tSubfieldsInReverseDiff" +
            "\tReverseDiffCount" +
            "\tOverlapCount" +
            "\tAntalBibdelfält" +
            "\tAntalAuktdelfält" +
            "\tHasPartialD" +
            "\tbibdelfält" +
            "\tauktdelfält" +
            "\thas240a" +
            "\tbibId" +
            "\tauktId" +
            "\tauktUrl" +
            "\tbibUrl" +
            "\n"

    synchronized void appendtofile(BufferedWriter fileWriter, matches) {
        matches.each { match ->
            fileWriter.write("${match.type}" +
                    "\t${match.diff.count { it }}" +
                    "\t${match.spec.field}" +
                    "\t${match.bibField}" +
                    "\t${match.subfieldsInOverlap}" +
                    "\t${match.subfieldsInDiff}" +
                    "\t${match.subfieldsInReversediff}" +
                    "\t${match.reverseDiff.count { it }}" +
                    "\t${match.overlap.count { it }}" +
                    "\t${match.numBibFields}" +
                    "\t${match.numAuthFields}" +
                    "\t${match.partialD}" +
                    "\t${match.bibSet} " +
                    "\t${match.authSet} " +
                    "\t${match.bibHas240a}" +
                    "\t${match.bibId} " +
                    "\t${match.authId} " +
                    "\thttp://data.libris.kb.se/auth/oaipmh?verb=GetRecord&metadataPrefix=marcxml&identifier=http://libris.kb.se/resource/auth/${match.authId}" +
                    "\thttp://data.libris.kb.se/bib/oaipmh?verb=GetRecord&metadataPrefix=marcxml&identifier=http://libris.kb.se/resource/bib/${match.bibId}" +
                    "\n")

        }
    }
}

