package whelk.importer.libris

import spock.lang.Specification


class OaiPmhImporterSpec extends Specification {

    static BASE = "http://example.org/service"

    def importer = new OaiPmhImporter(serviceUrl: BASE) {
        def documents = []
        void addDocuments(final List documents) {
            this.documents += documents
        }
    }

    def "should import OAI-PMH"() {
        when:
        importer.doImport("dataset")
        then:
        importer.documents.size() == 2
    }

    static {
        URL.metaClass.getText = {
            assert pages.keySet()[0].class == String
            pages[delegate.toString()]
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

    static pages = [
        (BASE+'?verb=ListRecords&resumptionToken=null'): oaiPmhPage("""
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

}
