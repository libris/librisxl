package whelk.harvester

import whelk.Document
import whelk.Location
import whelk.Whelk

import spock.lang.Specification
import groovy.util.logging.Slf4j as Log
import whelk.component.PostgreSQLComponent
import whelk.component.Storage
import whelk.converter.marc.MarcFrameConverter

/*@Log
class OaiPmhHarvesterSpec extends Specification {

    static BASE = "http://example.org/service"

    static currentPageSet = null

    static {
        URL.metaClass.getText = {
            currentPageSet[delegate.toString()]
        }
        URL.metaClass.openStream = {
            new ByteArrayInputStream( currentPageSet[delegate.toString()].getBytes("UTF-8") )
        }
    }

    static date(dateStr) {
        Date.parse(OaiPmhHarvester.DATE_FORMAT, dateStr)
    }

    def importer
    def whelk

    def setup() {
        Storage storage = GroovyMock(Storage.class)
        whelk = new Whelk() {
            @Override
            void bulkStore(final List<Document> documents,
                    String changedIn, String changedBy, String collection,
                    boolean createOrUpdate) {
                this.documents += documents
            }
            @Override
            void remove(String id) {
            }
            Storage getStorage() {
                return new PostgreSQLComponent() {
                    Location locate(String id) {
                        return new Location().withId(id)
                    }
                }
            }

            def documents = []
        }
        importer = new LibrisOaiPmhHarvester(whelk, new MarcFrameConverter()) {
            def documents = []
            void addDocuments(final List documents) {
                this.documents += documents
                super.addDocuments(documents)
            }
        }
    }

    def "should import OAI-PMH"() {
        given:
        currentPageSet = okPageSet
        when:
        def result = importer.harvest(BASE, null, "ListRecords", "marcxml")
        then:
        whelk.documents.size() == 2
        result.numberOfDocuments == 2
        result.numberOfDocumentsDeleted == 0
        result.lastRecordDatestamp == date("2002-02-02T00:00:00Z")
        new URL(whelk.documents.get(0).recordIdentifiers[1]).getPath() == "/auth/1"
        new URL(whelk.documents.get(1).recordIdentifiers[1]).getPath() == "/auth/2"
    }

    def "should follow resumptionToken even from empty page"() {
        given:
        currentPageSet = emptyFirstPagePageSet
        when:
        def result = importer.harvest(BASE, null, "ListRecords", "marcxml")
        then:
        result.numberOfDocuments == 1
        result.lastRecordDatestamp == date("2002-02-02T00:00:00Z")
    }

    def "should retain last successful record datestamp"() {
        given:
        currentPageSet = brokenPageSet
        when:
        def result = importer.harvest(BASE, null, "ListRecords", "marcxml")
        then:
        result.lastRecordDatestamp == date("2002-02-02T00:00:00Z")
    }

    def "should skip records originating from itself"() {
        given:
        currentPageSet = selfOriginPageSet
        when:
        def result = importer.harvest(BASE, null, "ListRecords", "marcxml")
        then:
        result.numberOfDocuments == 0
        result.numberOfDocumentsSkipped == 1
        result.lastRecordDatestamp == date("2007-01-01T00:00:00Z")
    }

    def "should disregard suppressed records"() {
        given:
        currentPageSet = suppressedPageSet
        when:
        def result = importer.harvest(BASE, null, "ListRecords", "marcxml")
        then:
        result.numberOfDocuments == 0
        result.numberOfDocumentsSkipped == 1
        result.lastRecordDatestamp == date("2015-05-28T12:43:00Z")
    }

    def "should not accept lastRecordDatestamp lower than start date"() {
        given:
        currentPageSet = badDateOrderPageSet
        when: "one is correct"
        def result = importer.harvest(BASE, null, "ListRecords", "marcxml", date("2015-05-29T00:00:00Z"))
        then:
        result.numberOfDocuments == 1
        result.numberOfDocumentsDeleted == 0
        result.lastRecordDatestamp == date("2015-05-29T08:00:00Z")
    }

    def "should preserve record datestamps until bad"() {
        given:
        badDateOrderPageSet[(BASE+'?verb=ListRecords&metadataPrefix=marcxml&from=2015-05-29T09:00:00Z')] = badDateOrderPageSet[(BASE+'?verb=ListRecords&metadataPrefix=marcxml&from=2015-05-29T00:00:00Z')]
        currentPageSet = badDateOrderPageSet
        when: "none is correct"
        def result = importer.harvest(BASE, null, "ListRecords", "marcxml", date("2015-05-29T09:00:00Z"))
        then:
        result.numberOfDocuments == 0
        result.numberOfDocumentsDeleted == 0
        result.lastRecordDatestamp == date("2015-05-29T09:00:00Z")
    }

    def "should handle delete records"() {
        given:
        currentPageSet = deleteRecord
        when:
        def result = importer.harvest(BASE, null, "ListRecords", "marcxml")
        then:
        result.numberOfDocuments == 1
        result.numberOfDocumentsDeleted == 1
    }

    static oaiPmhPage(body) {
        """<?xml version="1.0"?>
            <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
                <responseDate>1970-01-01T00:00:00Z</responseDate>
                <ListRecords>
                    $body
                </ListRecords>
            </OAI-PMH>
        """
    }

    static okPageSet = [
        (BASE+'?verb=ListRecords&metadataPrefix=marcxml'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/1</identifier>
                    <datestamp>2001-01-01T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                    <leader>00986cz  a2200217n  4500</leader>
                    <controlfield tag="001">1</controlfield>
                    </record>
                </metadata>
            </record>
            <resumptionToken>page-2</resumptionToken>
        """),
        (BASE+'?verb=ListRecords&resumptionToken=page-2'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/2</identifier>
                    <datestamp>2002-02-02T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                    <leader>00986cz  a2200217n  4500</leader>
                    <controlfield tag="001">2</controlfield>
                    </record>
                </metadata>
            </record>
        """)
    ]

    static deleteRecord = [
            (BASE+'?verb=ListRecords&metadataPrefix=marcxml'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/1</identifier>
                    <datestamp>2001-01-01T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                    <status>deleted</status>
                </header>
            </record>
            <resumptionToken>page-2</resumptionToken>
        """),
            (BASE+'?verb=ListRecords&resumptionToken=page-2'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/2</identifier>
                    <datestamp>2002-02-02T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                    <leader>00986cz  a2200217n  4500</leader>
                    <controlfield tag="001">2</controlfield>
                    </record>
                </metadata>
            </record>
        """)
    ]

    static emptyFirstPagePageSet = [
        (BASE+'?verb=ListRecords&metadataPrefix=marcxml'): oaiPmhPage("""
            <resumptionToken>page-2</resumptionToken>
        """),
        (BASE+'?verb=ListRecords&resumptionToken=page-2'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/2</identifier>
                    <datestamp>2002-02-02T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                    <leader>00986cz  a2200217n  4500</leader>
                    <controlfield tag="001">2</controlfield>
                    </record>
                </metadata>
            </record>
        """)
    ]

    static brokenPageSet = [
        (BASE+'?verb=ListRecords&metadataPrefix=marcxml'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/1</identifier>
                    <datestamp>2001-01-01T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                        <leader>00986cz  a2200217n  4500</leader>
                        <controlfield tag="001">1</controlfield>
                    </record>
                </metadata>
            </record>
            <resumptionToken>page-2</resumptionToken>
        """),
        (BASE+'?verb=ListRecords&resumptionToken=page-2'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/2</identifier>
                    <datestamp>2002-02-02T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                        <leader>00986cz  a2200217n  4500</leader>
                        <controlfield tag="001">2</controlfield>
                    </record>
                </metadata>
            </record>
            <record>
                <header>
                    <identifier>http://example.org/item/3</identifier>
                    <datestamp>2003-03-03T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <!-- MISSING RECORD... -->
                </metadata>
            </record>
            <record>
                <header>
                    <identifier>http://example.org/item/4</identifier>
                    <datestamp>2004-04-04T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                        <leader>00986cz  a2200217n  4500</leader>
                        <controlfield tag="001">4</controlfield>
                    </record>
                </metadata>
            </record>
        """)
    ]

    static selfOriginPageSet = [
        (BASE+'?verb=ListRecords&metadataPrefix=marcxml'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/1</identifier>
                    <datestamp>2007-01-01T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                    <leader>00986cz  a2200217n  4500</leader>
                    <controlfield tag="001">1</controlfield>
                    <controlfield tag="005">19700101010000.0</controlfield>
                    <datafield tag="887" ind1=" " ind2=" ">
                        <subfield code="a">{"@id": "http://example.org/item/1", "modified": 1167609600000}</subfield>
                        <subfield code="2">librisxl</subfield>
                    </datafield>
                    </record>
                </metadata>
            </record>
        """)
    ]

    static suppressedPageSet = [
        (BASE+'?verb=ListRecords&metadataPrefix=marcxml'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/1</identifier>
                    <datestamp>2015-05-28T12:43:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                    <leader>00986cz  a2200217n  4500</leader>
                    <controlfield tag="001">1</controlfield>
                    <controlfield tag="005">20150528124300.0</controlfield>
                    <datafield tag="599" ind1=" " ind2=" ">
                        <subfield code="a">SUPPRESSRECORD</subfield>
                    </datafield>
                    </record>
                </metadata>
            </record>
        """)
    ]

    static badDateOrderPageSet = [
        (BASE+'?verb=ListRecords&metadataPrefix=marcxml&from=2015-05-29T00:00:00Z'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/1</identifier>
                    <datestamp>2015-05-29T08:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                    <leader>00986cz  a2200217n  4500</leader>
                    <controlfield tag="001">1</controlfield>
                    </record>
                </metadata>
            </record>
            <resumptionToken>page-2</resumptionToken>
        """),
        (BASE+'?verb=ListRecords&resumptionToken=page-2'): oaiPmhPage("""
            <record>
                <header>
                    <identifier>http://example.org/item/2</identifier>
                    <datestamp>2002-02-02T00:00:00Z</datestamp>
                    <setSpec>license:CC0</setSpec>
                </header>
                <metadata>
                    <record xmlns="http://www.loc.gov/MARC21/slim" type="Authority">
                    <leader>00986cz  a2200217n  4500</leader>
                    <controlfield tag="001">2</controlfield>
                    </record>
                </metadata>
            </record>
        """)
    ]
}*/
