package whelk.actors
import whelk.AuthBibMatcher
import whelk.util.VCopyToWhelkConverter
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
                List<Map> allAuthRecords = VCopyToWhelkConverter.getAuthDocsFromRows(rows)
                resultMap.allAuth = allAuthRecords.size()
                Map doc = VCopyToWhelkConverter.getMarcDocMap(row.data)
                if (doc == null)
                    docIsNull++

                if (doc != null) {
                    List<Map> matchResults = AuthBibMatcher.matchAuthToBib(doc, allAuthRecords)

                    def uncertainMatches = matchResults.findAll { Map match ->
                        ((match.hasOnlyDiff || match.hasOnlyReverseDiff || match.hasDoubleDiff)
                                && !match.hasMisMatchOnA && !match.isMatch)
                    }

                    def completeMatches = matchResults.findAll { it.isMatch }
                    //TODO: handle writes to resultmap in threaded environment.
                    resultMap.matches += completeMatches.size()

                    populateResults(matchResults, uncertainMatches)
                    printDiffResultsToFile(uncertainMatches, completeMatches)

                }
            }
        })
    }

    synchronized void populateResults(matchResults, uncertainMatches){
        matchResults.findAll { Map match ->
            !match.hasOnlyDiff &&
                    !match.hasOnlyReverseDiff &&
                    !match.hasDoubleDiff &&
                    !match.hasMisMatchOnA &&
                    !match.isMatch
        }.each { diff ->
            log.debug "miss! Diff: ${diff.inspect()}"
        }

        def misMatchesOnA = matchResults.findAll { it.hasMisMatchOnA }

        resultMap.possibleMatches += matchResults.count { it }
        resultMap.MisMatchesOnA += misMatchesOnA.size()
        resultMap.bibInAukt += uncertainMatches.count { it.hasOnlyDiff }
        resultMap.auktInBib += uncertainMatches.count { it.hasOnlyReverseDiff }
        resultMap.doubleDiff += uncertainMatches.count { it.hasDoubleDiff }
        resultMap.specAndDoc++
        resultMap.MissingBibFields = missingBibFields
    }

    synchronized void printDiffResultsToFile(uncertainMatches, completeMatches) {
        appendtofile(uncertainFileWriter, uncertainMatches)
        appendtofile(completeFileWriter, completeMatches)
    }

    String header = "MatchType" +
            "\tMatchPattern" +
            "\tAuktfält" +
            "\tBibfält" +
            "\tAllaDelfält" +
            "\tHasPartialD" +
            "\tauktdelfält" +
            "\tbibdelfält" +
            "\thas240a" +
            "\tbibId" +
            "\tauktId" +
            "\tauktUrl" +
            "\tbibUrl" +
            "\n"

    synchronized void appendtofile(BufferedWriter fileWriter, matches) {
        matches.each { Map match ->
            fileWriter.write("${match.type}" +
                    "\t${getMatchPattern(match)}" +
                    "\t${match.spec.field}" +
                    "\t${match.bibField}" +
                    "\t${formatSubfieldDiff(match.subfieldsInDiff)} - ${formatSubfieldDiff(match.subfieldsInOverlap)} - ${formatSubfieldDiff(match.subfieldsInReversediff)}" +
                    "\t${match.partialD}" +
                    "\t${match.authSet} " +
                    "\t${match.bibSet} " +
                    "\t${match.bibHas240a}" +
                    "\t${match.bibId} " +
                    "\t${match.authId} " +
                    "\thttp://data.libris.kb.se/auth/oaipmh?verb=GetRecord&metadataPrefix=marcxml&identifier=http://libris.kb.se/resource/auth/${match.authId}" +
                    "\thttp://data.libris.kb.se/bib/oaipmh?verb=GetRecord&metadataPrefix=marcxml&identifier=http://libris.kb.se/resource/bib/${match.bibId}" +
                    "\n")
        }
    }

    static String getMatchPattern(match){
        def formatPropery = { String s -> s ? s : '_' }
        def r = formatPropery(match.subfieldsInDiff) + '-' + formatPropery(match.subfieldsInOverlap) + '-' + formatPropery(match.subfieldsInReversediff)
        return r
    }

    static String formatSubfieldDiff(String diff) {
        if (!diff) {
            return '_'
        } else {
            return diff
        }
    }
}

