package whelk.util

import se.kb.libris.util.marc.MarcRecord
import se.kb.libris.util.marc.impl.ControlfieldImpl
import se.kb.libris.util.marc.impl.DatafieldImpl
import se.kb.libris.util.marc.impl.MarcRecordImpl
import se.kb.libris.util.marc.impl.SubfieldImpl
import spock.lang.Specification

import whelk.JsonLd

class LegacyIntegrationToolsSpec extends Specification {

    static final Map CONTEXT_DATA = [
        "@context": ["@vocab": "http://example.org/ns/"]
    ]

    static final String MARC = 'https://id.kb.se/marc'

    static final Map VOCAB_DATA = [
        "@graph": [
            ["@id": "http://example.org/ns/Instance",
             "category": [ ["@id": "$MARC/bib"] ]],
            ["@id": "http://example.org/ns/Print",
             "subClassOf": [ ["@id": "http://example.org/ns/Instance"] ],
             "category": [ ["@id": "http://example.org/ns/"] ]],
            ["@id": "http://example.org/ns/Paperback",
             "subClassOf": [ ["@id": "http://example.org/ns/Print"] ],
             "category": ["@id": "http://example.org/ns/pending"]],
            ["@id": "http://example.org/ns/None",
             "subClassOf": [ ["@id": "http://example.org/ns/Instance"] ],
             "category": [ ["@id": "$MARC/none"] ]],
        ]
    ]

    def tool = new LegacyIntegrationTools()

    def "should get marc category for term"() {
        expect:
        tool.getMarcCollectionForTerm([category: cats]) == id
        where:
        id          | cats
        'bib'       | ['@id': "$MARC/bib"]
        'bib'       | [['@id': "$MARC/bib"], ['@id': "pending"]]
        'auth'      | [['@id': "$MARC/auth"]]
        'undefined' | ['@id': 'other']
        'undefined' | [['@id': 'other']]
        'undefined' | []
        'undefined' | null
    }

    def "should get marc collection for type"() {
        expect:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        tool.getMarcCollectionInHierarchy(type, ld) == collection
        where:
        type        | collection
        'Instance'  | 'bib'
        'Print'     | 'bib'
        'Paperback' | 'bib'
        'Other'     | 'none'
        'None'      | 'none'
    }

    def "legacySigelToUri"() {
        expect:
        tool.legacySigelToUri(sigel) == uri
        where:
        sigel                              | uri
        'S'                                | 'https://libris.kb.se/library/S'
        'Utb1'                             | 'https://libris.kb.se/library/Utb1'
        'Ö 1'                              | 'https://libris.kb.se/library/%C3%96+1'
        'https://libris.kb.se/library/S'   | 'https://libris.kb.se/library/S' // already a URI
    }

    def "uriToLegacySigel"() {
        expect:
        tool.uriToLegacySigel(uri) == sigel
        where:
        uri                                   | sigel
        'https://libris.kb.se/library/S'      | 'S'
        'https://libris.kb.se/library/Utb1'   | 'Utb1'
        'https://libris.kb.se/library/%C3%96+1' | 'Ö 1'
        'https://example.org/library/S'       | null
        'S'                                   | null
    }

    def "sigel survives URI round trip"() {
        expect:
        tool.uriToLegacySigel(tool.legacySigelToUri(sigel)) == sigel
        where:
        sigel << ['S', 'Utb1', 'Ö 1', 'å/ä', 'X&Y', 'A+B']
    }

    def "fixUri restores double slash stripped by Tomcat"() {
        expect:
        tool.fixUri(uri) == fixed
        where:
        uri                        | fixed
        '/http:/example.org/x'     | '/http://example.org/x'
        '/https:/example.org/x'    | '/https://example.org/x'
        '/http://example.org/x'    | '/http://example.org/x'
        '/https://example.org/x'   | '/https://example.org/x'
        '/find?q=x'                | '/find?q=x'
        ''                         | ''
    }

    def "makeRecordLibrisResident moves foreign id to 035 and sets 003 to SE-LIBR"() {
        given:
        MarcRecord record = new MarcRecordImpl()
        record.addField(new ControlfieldImpl('001', '12345'))
        record.addField(new ControlfieldImpl('003', 'OTHER'))

        when:
        tool.makeRecordLibrisResident(record)

        then:
        get035a(record) == ['(OTHER)12345']
        record.getControlfields('003').collect { it.data } == ['SE-LIBR']
    }

    def "makeRecordLibrisResident does not add duplicate 035"() {
        given:
        MarcRecord record = new MarcRecordImpl()
        record.addField(new ControlfieldImpl('001', '12345'))
        record.addField(new ControlfieldImpl('003', 'OTHER'))
        def field035 = new DatafieldImpl('035')
        field035.addSubfield(new SubfieldImpl('a' as char, '(OTHER)12345'))
        record.addField(field035)

        when:
        tool.makeRecordLibrisResident(record)

        then:
        get035a(record) == ['(OTHER)12345']
        record.getControlfields('003').collect { it.data } == ['SE-LIBR']
    }

    def "makeRecordLibrisResident does not add 035 for LIBRIS records"() {
        given:
        MarcRecord record = new MarcRecordImpl()
        record.addField(new ControlfieldImpl('001', '12345'))
        record.addField(new ControlfieldImpl('003', libris003))

        when:
        tool.makeRecordLibrisResident(record)

        then:
        get035a(record) == []
        record.getControlfields('003').collect { it.data } == ['SE-LIBR']

        where:
        libris003 << ['SE-LIBR', 'LIBRIS']
    }

    def "makeRecordLibrisResident replaces all existing 003"() {
        given:
        MarcRecord record = new MarcRecordImpl()
        record.addField(new ControlfieldImpl('001', '12345'))
        record.addField(new ControlfieldImpl('003', 'OTHER'))
        record.addField(new ControlfieldImpl('003', 'ANOTHER'))

        when:
        tool.makeRecordLibrisResident(record)

        then:
        record.getControlfields('003').collect { it.data } == ['SE-LIBR']
    }

    def "makeRecordLibrisResident handles record without 001 and 003"() {
        given:
        MarcRecord record = new MarcRecordImpl()

        when:
        tool.makeRecordLibrisResident(record)

        then:
        get035a(record) == []
        record.getControlfields('003').collect { it.data } == ['SE-LIBR']
    }

    private static List<String> get035a(MarcRecord record) {
        record.getDatafields('035').collectMany { f -> f.getSubfields('a').collect { it.data } }
    }
}
