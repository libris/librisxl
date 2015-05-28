package whelk.importer.libris

import spock.lang.Specification


class OaiPmhImporterSpec extends Specification {

    static DATE_FORMAT = OaiPmhImporter.DATE_FORMAT

    static BASE = "http://example.org/service"

    def importer = new OaiPmhImporter(serviceUrl: BASE) {
        def documents = []
        void addDocuments(final List documents) {
            this.documents += documents
        }
    }

    def "should import OAI-PMH"() {
        given:
        currentPageSet = okPageSet
        def expectedLast = Date.parse(DATE_FORMAT, "2002-02-02T00:00:00Z")
        when:
        def result = importer.doImport("dataset")
        then:
        importer.documents.size() == 2
        result.numberOfDocuments == 2
        result.numberOfDeleted == 0
        result.lastRecordDatestamp == expectedLast
    }

    def "should retain last successful record datestamp"() {
        given:
        currentPageSet = brokenPageSet
        def expectedLast = Date.parse(DATE_FORMAT, "2002-02-02T00:00:00Z")
        when:
        def result = importer.doImport("dataset")
        then:
        result.lastRecordDatestamp == expectedLast
    }

    static {
        URL.metaClass.getText = {
            currentPageSet[delegate.toString()]
        }
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

    static currentPageSet = null

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

}
