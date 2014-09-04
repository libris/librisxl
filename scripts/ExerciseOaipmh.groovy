

class ExerciseOaipmh {

    void parseOaipmh(startUrl, name, passwd) {
        getAuthentication(name, passwd)
        String resumptionToken = null
        def startTime = System.currentTimeMillis()
        def recordCount = 0
        while (true) {
            def url = makeNextUrl(startUrl, resumptionToken)

            def netTimeStart = System.currentTimeMillis()
            def xmlString = new URL(url).text // NOTE: might be bad UTF-8
            def netTime = System.currentTimeMillis() - netTimeStart

            def parseTimeStart = System.currentTimeMillis()
            def doc = new XmlSlurper(false,false).parseText(xmlString)
            def parseTime = System.currentTimeMillis() - parseTimeStart

            resumptionToken = doc.ListRecords.resumptionToken?.text()

            recordCount += doc.ListRecords.record.size()
            def elapsed = (System.currentTimeMillis() - startTime) / 1000
            println "Record count: $recordCount. Got resumption token: $resumptionToken. Elapsed time: $elapsed. Records/second: ${recordCount / elapsed}. Net/net+parse time ratio: ${netTime/(netTime + parseTime)}"
            if (!resumptionToken)
                break
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
        def startUrl = "http://data.libris.kb.se/hold/oaipmh"
        def name = args[0]
        def passwd = args[1]
        new ExerciseOaipmh().parseOaipmh(startUrl, name, passwd)
    }
}
