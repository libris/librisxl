import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult

class ExerciseOaipmh {

    int recordCount

    void parseOaipmh(startUrl, name, passwd) {
        getAuthentication(name, passwd)
        String resumptionToken = null
        def startTime = System.currentTimeMillis()
        recordCount = 0
        while (true) {
            def batchTime = System.currentTimeMillis()
            def url = makeNextUrl(startUrl, resumptionToken)

            def netTimeStart = System.currentTimeMillis()
            def xmlString = new URL(url).text // NOTE: might be bad UTF-8
            def netTime = System.currentTimeMillis() - netTimeStart

            def parseTimeStart = System.currentTimeMillis()
            def doc = new XmlSlurper(false,false).parseText(xmlString)
            def parseTime = System.currentTimeMillis() - parseTimeStart

            resumptionToken = doc.ListRecords.resumptionToken?.text()

            def elapsed = (System.currentTimeMillis() - startTime) / 1000
            def batchCount = doc.ListRecords.record.size()
            recordCount += batchCount
            int docsPerSecond = batchCount/((System.currentTimeMillis() - batchTime) / 1000)
            println "Record count: $recordCount. Got resumption token: $resumptionToken. Elapsed time: $elapsed. Records/second: ${recordCount / elapsed}. Net/net+parse time ratio: ${netTime/(netTime + parseTime)}, docspersecond: $docsPerSecond"
            if (!resumptionToken) {
                println "Done, after $elapsed seconds."
                break
            }
        }
    }

    String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind{
            out << root
        }
    }


    def makeNextUrl(startUrl, resumptionToken) {
        def params = resumptionToken?
            "?verb=ListRecords&resumptionToken=" + resumptionToken :
            "?verb=ListRecords&metadataPrefix=marcxml"
        return startUrl + params
    }

    private void getAuthentication(username, password) {
        Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password.toCharArray())
                }
            });
    }

    static void main(args) {
        def startUrl = args[0]
        def name = args[1]
        def passwd = args[2]
        new ExerciseOaipmh().parseOaipmh(startUrl, name, passwd)
    }
}
