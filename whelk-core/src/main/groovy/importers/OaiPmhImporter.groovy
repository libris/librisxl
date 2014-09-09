package se.kb.libris.whelks.importers

import groovy.util.logging.Slf4j as Log
import groovy.xml.StreamingMarkupBuilder
import groovy.util.slurpersupport.GPathResult

@Log
abstract class OaiPmhImporter {

    int recordCount

    void parseOaipmh(startUrl, name, passwd, since) {
        getAuthentication(name, passwd)
        String resumptionToken = null
        def startTime = System.currentTimeMillis()
        recordCount = 0
        while (true) {
            def batchTime = System.currentTimeMillis()
            def url = makeNextUrl(startUrl, resumptionToken, since)
            log.debug("Harvesting from $url")
            def xmlString = new URL(url).text // NOTE: might be bad UTF-8
            def doc = new XmlSlurper(false,false).parseText(xmlString)
            parseResult(doc)
            resumptionToken = doc.ListRecords.resumptionToken?.text()

            def elapsed = (System.currentTimeMillis() - startTime) / 1000
            def batchCount = doc.ListRecords.record.size()
            recordCount += batchCount
            int docsPerSecond = batchCount/((System.currentTimeMillis() - batchTime) / 1000)
            println("Record count: ${recordCount}. Got resumption token: ${resumptionToken}. Elapsed time: ${elapsed}. Records/second: $docsPerSecond")
            if (!resumptionToken) {
                println "Done, after $elapsed seconds."
                break
            }
        }
    }

    def makeNextUrl(startUrl, resumptionToken, since) {
        def params
        if (resumptionToken) {
            params = "?verb=ListRecords&resumptionToken=" + resumptionToken
        } else {
            params = "?verb=ListRecords&metadataPrefix=marcxml"
            if (since) {
                params += "&from=" + since.format("yyyy-MM-dd'T'HH:mm:ss'Z'")
            }
        }
        return startUrl + params
    }

    private void getAuthentication(username, password) {
        Authenticator.setDefault(new Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password.toCharArray())
                }
            });
    }

    abstract void parseResult(final slurpedXml)

    String createString(GPathResult root) {
        return new StreamingMarkupBuilder().bind{
            out << root
        }
    }
}
