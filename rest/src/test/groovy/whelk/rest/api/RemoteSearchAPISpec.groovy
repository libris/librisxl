package whelk.rest.api

import spock.lang.Specification
import whelk.Document

class RemoteSearchAPISpec extends Specification {
    RemoteSearchAPI remote

    void setup() {
        remote = new RemoteSearchAPI()
    }

    def "Merge results should return formatted output"() {
        given:
        def doc = new Document(['@graph': [['@id': "testId"]]])
        def resultBNB = createResult("BNB", 1, null, [doc])
        def resultOCLC = createResult("OCLC", 1, null, [doc])
        def resultLists = [resultOCLC, resultBNB]

        when:
        def output = remote.mergeResults(resultLists)

        then:
        output == '{"totalResults":{"OCLC":1,"BNB":1},"items":[{"database":"OCLC","data":{"@graph":[{"@id":"testId"}]}},{"database":"BNB","data":{"@graph":[{"@id":"testId"}]}}]}'
    }

    def "Merge result should not be grouped by database"() {
        given:
        def doc1 = new Document(['@graph': [['@id': "testId"]]])
        def doc2 = new Document(['@graph': [['@id': "testId2"]]])
        def doc3 = new Document(['@graph': [['@id': "testId3"]]])
        def resultOCLC = createResult("OCLC", 1, null, [doc1])
        def resultBNB = createResult("BNB", 2, null, [doc1, doc2])
        def resultCORNELL = createResult("CORNELL", 3, null, [doc1, doc2, doc3])
        def resultLists = [resultOCLC, resultBNB, resultCORNELL]

        when:
        def output = remote.mergeResults(resultLists)

        then:
        output  == '{"totalResults":{"OCLC":1,"BNB":2,"CORNELL":3},"items":[{"database":"OCLC","data":{"@graph":[{"@id":"testId"}]}},{"database":"BNB","data":{"@graph":[{"@id":"testId"}]}},{"database":"CORNELL","data":{"@graph":[{"@id":"testId"}]}},{"database":"BNB","data":{"@graph":[{"@id":"testId2"}]}},{"database":"CORNELL","data":{"@graph":[{"@id":"testId2"}]}},{"database":"CORNELL","data":{"@graph":[{"@id":"testId3"}]}}]}'
    }

    def "Merge results should append error message to output"() {
        given:
        def doc = new Document(['@graph': [['@id': "testId"]]])
        def resultBNB = createResult("BNB", 1, "Error message", [doc])
        def resultOCLC = createResult("OCLC", 1, null, [doc])
        def resultLists = [resultOCLC, resultBNB]

        when:
        def output = remote.mergeResults(resultLists)

        then:
        output == '{"totalResults":{"OCLC":1,"BNB":1},"items":[{"database":"OCLC","data":{"@graph":[{"@id":"testId"}]}}],"errors":{"BNB":{"0":"Error message"}}}'
    }

    def "Merge results should handle empty results"() {
        given:
        def resultBNB = createResult("BNB", 0, null, [])
        def resultOCLC = createResult("OCLC", 0, null, [])
        def resultLists = [resultOCLC, resultBNB]

        when:
        def output = remote.mergeResults(resultLists)

        then:
        output == '{"totalResults":{"OCLC":0,"BNB":0},"items":[]}'
    }

    def createResult(String database, int numberOfHits, error, docs) {
        def result = new RemoteSearchAPI.MetaproxySearchResult(null, database, numberOfHits)
        result.error = error
        for (doc in docs) {
            result.addHit(doc)
        }
        return result
    }
}

