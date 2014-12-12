package whelk.result

import groovy.util.logging.Slf4j
import groovy.xml.StreamingMarkupBuilder
import whelk.Document
import whelk.converter.JsonLD2MarcXMLConverter

/**
 * Created by Markus Holm on 14-12-12.
 */
@Slf4j
class OaiPmhSearchResult extends SearchResult {

    JsonLD2MarcXMLConverter jsonLD2MarcXMLConverter = null
    String toXml(String resultKey, List keys) {
        //def result = ["@context":"/sys/context/lib.jsonld", "startIndex":startIndex, "itemsPerPage": resultSize, "totalResults": numberOfHits, "duration": searchCompletedInISO8601duration, "items": [] ]

        // Get document convert with jsonld2Marcxml
        // Pad with info

        if (jsonLD2MarcXMLConverter == null )
            jsonLD2MarcXMLConverter = new JsonLD2MarcXMLConverter()

        def result              = new StringWriter()
        def builder             = new StreamingMarkupBuilder()
        def defualtNamespace    = 'http://www.openarchives.org/OAI/2.0/'
        def xsiNamespace        = 'http://www.w3.org/2001/XMLSchema-instance'
        def schemaLocation      = 'http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd'
        def root = {
            mkp.xmlDeclaration()
            mkp.declareNamespace('':defualtNamespace)
            mkp.declareNamespace('xsi':xsiNamespace)
            "OAI-PMH"('xsi:schemaLocation':schemaLocation) {
                responseDate(new Date().format("yyyy-MM-dd'T'HH:MM:ss'Z'"))
                request("verb":"ListRecords", "metadataPrefix":"oai_dc", "set":"mussm", "http://memory.loc.gov/cgi-bin/oai2_0")
                hits.each {
                    record() {
                        header() {
                            identifier("oai:arXiv:cs/0112017")
                            datestamp("2002-02-28") //datestamp of the record
                            setSpec("cs") //optinal
                        }
                        metadata() {
                            mkp.declareNamespace('marc':'http://www.loc.gov/MARC21/slim')
                            mkp.declareNamespace('xsi':'http://www.w3.org/2001/XMLSchema-instance')
                        }
                    }
                }
                ListRecords() {
                }
                resumptionToken("completeListSize":"62976", "cursor": "0", "4Jle")
            }
        }

        result << builder.bind(root)


        System.out.println(result.toString())
        return result.toString()
    }
    @Override
    void addHit(Document doc, Map<String, String[]> highlightedFields) {
        doc.meta['matches'] = highlightedFields
        jsonLD2MarcXMLConverter.doConvert(doc)
        this.hits.add(doc)
    }
}