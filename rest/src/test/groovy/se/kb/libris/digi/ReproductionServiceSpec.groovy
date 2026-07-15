package se.kb.libris.digi

import spock.lang.Specification
import spock.lang.Unroll

/**
 * Behavioural tests for ReproductionService, driven through a recording fake XL that serves
 * canned records and captures the record/thing payloads that would be POSTed. No live HTTP.
 */
class ReproductionServiceSpec extends Specification {

    static final String PHYS = 'http://x/phys'
    static final String LIB_S = 'https://libris.kb.se/library/S'
    static final String DIGI = 'https://libris.kb.se/library/DIGI'
    static final String DST = 'https://libris.kb.se/library/DST'
    static final String ONLINE = 'https://id.kb.se/term/rda/OnlineResource'
    static final String FREELY_AVAILABLE = 'https://id.kb.se/policy/freely-available'

    def "creates a minimal reproduction: copies title/instanceOf from original and adds DIGI"() {
        given:
        def xl = new FakeXL([(PHYS): physicalInstance()])

        when:
        def id = new ReproductionService(xl).createDigitalReproduction(electronic(), false)

        then:
        id == 'http://created/1#it'
        xl.created.size() == 1
        def created = xl.created[0]
        created.record.bibliography == [['@id': DIGI]] as Set
        created.thing.instanceOf == ['@id': 'http://x/work#it']   // linked from original, not extracted
        created.thing.hasTitle == [['@type': 'Title', 'mainTitle': 'T']]  // copied from original
    }

    def "adds carrierType OnlineResource for an online Electronic"() {
        given:
        def xl = new FakeXL([(PHYS): physicalInstance()])

        when:
        new ReproductionService(xl).createDigitalReproduction(onlineElectronic(), false)

        then:
        def thing = xl.created[0].thing
        thing.carrierType == [['@id': ONLINE]] as Set
        !thing.containsKey('category')
    }

    def "adds category OnlineResource (not carrierType) for an online DigitalResource"() {
        given:
        def xl = new FakeXL([(PHYS): physicalInstance()])
        def input = onlineElectronic() + ['@type': 'DigitalResource']

        when:
        new ReproductionService(xl).createDigitalReproduction(input, false)

        then:
        def thing = xl.created[0].thing
        thing.category == [['@id': ONLINE]] as Set
        !thing.containsKey('carrierType')
    }

    def "keeps a bibliography supplied in meta and appends DIGI"() {
        given:
        def xl = new FakeXL([(PHYS): physicalInstance()])
        def input = electronic() + ['meta': ['bibliography': [['@id': 'https://libris.kb.se/library/ARB']]]]

        when:
        new ReproductionService(xl).createDigitalReproduction(input, false)

        then:
        xl.created[0].record.bibliography == [['@id': 'https://libris.kb.se/library/ARB'], ['@id': DIGI]] as Set
    }

    def "adds DST for freely available digitised Swedish print"() {
        given:
        def phys = physicalInstance() + ['publication': [['country': ['@id': 'https://id.kb.se/country/sw']]]]
        def xl = new FakeXL([(PHYS): phys])

        when:
        new ReproductionService(xl).createDigitalReproduction(onlineElectronic(), false)

        then:
        xl.created[0].record.bibliography == [['@id': DST], ['@id': DIGI]] as Set
    }

    def "does not add DST when not freely available"() {
        given:
        def phys = physicalInstance() + ['publication': [['country': ['@id': 'https://id.kb.se/country/sw']]]]
        def xl = new FakeXL([(PHYS): phys])

        when: 'electronic is not freely available (plain, no usageAndAccessPolicy)'
        new ReproductionService(xl).createDigitalReproduction(electronic(), false)

        then:
        xl.created[0].record.bibliography == [['@id': DIGI]] as Set
    }

    def "copies issuanceType from original only when the reproduction lacks one"() {
        given:
        def phys = physicalInstance() + ['issuanceType': 'Monograph']
        def xl = new FakeXL([(PHYS): phys])

        when:
        new ReproductionService(xl).createDigitalReproduction(electronic(), false)

        then:
        xl.created[0].thing.issuanceType == 'Monograph'
    }

    def "does not overwrite an issuanceType already on the reproduction"() {
        given:
        def phys = physicalInstance() + ['issuanceType': 'Monograph']
        def xl = new FakeXL([(PHYS): phys])
        def input = electronic() + ['issuanceType': 'Serial']

        when:
        new ReproductionService(xl).createDigitalReproduction(input, false)

        then:
        xl.created[0].thing.issuanceType == 'Serial'
    }

    def "creates holdings from @reverse.itemOf, lifting meta into the holding record"() {
        given:
        def xl = new FakeXL([(PHYS): physicalInstance(), (LIB_S): library()])
        def input = electronic() + ['@reverse': ['itemOf': [[
                'heldBy'         : ['@id': LIB_S],
                'cataloguersNote': ['bar'],
                'meta'           : ['cataloguersNote': ['baz']],
        ]]]]

        when:
        def id = new ReproductionService(xl).createDigitalReproduction(input, false)

        then: 'the reproduction is created first, then the holding'
        xl.created.size() == 2
        def holding = xl.created[1]
        holding.record == ['cataloguersNote': ['baz']]              // lifted out of meta
        holding.thing['@type'] == 'Item'
        holding.thing.itemOf == ['@id': id]
        holding.thing.heldBy == ['@id': LIB_S]
        holding.thing.cataloguersNote == ['bar']                    // non-meta data preserved
        holding.thing.hasComponent == [['@type': 'Item', 'heldBy': ['@id': LIB_S]]]  // default component
    }

    def "propagates heldBy into explicitly supplied holding components"() {
        given:
        def xl = new FakeXL([(PHYS): physicalInstance(), (LIB_S): library()])
        def input = electronic() + ['@reverse': ['itemOf': [[
                'heldBy'      : ['@id': LIB_S],
                'hasComponent': [['cataloguersNote': ['foo']]],
        ]]]]

        when:
        new ReproductionService(xl).createDigitalReproduction(input, false)

        then:
        xl.created[1].thing.hasComponent == [[
                'cataloguersNote': ['foo'],
                '@type'          : 'Item',
                'heldBy'         : ['@id': LIB_S],
        ]]
    }

    def "rejects a holding whose heldBy library does not exist"() {
        given:
        def xl = new FakeXL([(PHYS): physicalInstance()])  // library not served
        def input = electronic() + ['@reverse': ['itemOf': [['heldBy': ['@id': LIB_S]]]]]

        when:
        new ReproductionService(xl).createDigitalReproduction(input, false)

        then:
        def e = thrown(RequestException)
        e.code == 400
        e.msg == "No such library: [$LIB_S]"
        xl.created.isEmpty()
    }

    def "rejects reproductionOf pointing at a non-existent thing"() {
        given:
        def xl = new FakeXL([:])

        when:
        new ReproductionService(xl).createDigitalReproduction(electronic(), false)

        then:
        def e = thrown(RequestException)
        e.code == 400
        e.msg.startsWith('Thing linked in reproductionOf does not exist')
    }

    @Unroll
    def "rejects reproductionOf pointing at a #type"() {
        given:
        def xl = new FakeXL([(PHYS): ['@id': 'http://x/phys#it', '@type': type]])

        when:
        new ReproductionService(xl).createDigitalReproduction(electronic(), false)

        then:
        def e = thrown(RequestException)
        e.code == 400
        e.msg == "Thing linked in reproductionOf cannot be $type"

        where:
        type << ['Electronic', 'DigitalResource']
    }

    // --- extractWork path ---

    def "extractWork: creates a work from an inline original work, copying the title with a source ref"() {
        given:
        def phys = ['@id'       : 'http://x/phys#it', '@type': 'Instance',
                    'hasTitle'  : [['@type': 'Title', 'mainTitle': 'T']],
                    'instanceOf': ['@type': 'Work']]
        def xl = new FakeXL([(PHYS): phys])

        when:
        new ReproductionService(xl).createDigitalReproduction(electronic(), true)

        then: 'work is created first with the copied title, then the reproduction linking to it'
        xl.created.size() == 2
        def work = xl.created[0]
        work.record.generationProcess == ['@id': DigitalReproductionAPI.API_LOCATION]
        work.record.derivedFrom == [['@id': 'http://x/phys#it']]
        work.thing.hasTitle == [['@type': 'Title', 'mainTitle': 'T', 'source': [['@id': 'http://x/phys#it']]]]
        xl.created[1].thing.instanceOf == ['@id': 'http://created/1#it']

        and: 'the original instance is updated to link the extracted work'
        xl.updated.size() == 1
        xl.updated[0]['@graph'][1].instanceOf == ['@id': 'http://created/1#it']
    }

    def "extractWork: does not copy a title onto a work that already has one"() {
        given:
        def phys = ['@id'       : 'http://x/phys#it', '@type': 'Instance',
                    'hasTitle'  : [['@type': 'Title', 'mainTitle': 'T']],
                    'instanceOf': ['@type': 'Work', 'hasTitle': [['@type': 'Title', 'mainTitle': 'W']]]]
        def xl = new FakeXL([(PHYS): phys])

        when:
        new ReproductionService(xl).createDigitalReproduction(electronic(), true)

        then:
        xl.created[0].thing.hasTitle == [['@type': 'Title', 'mainTitle': 'W']]
    }

    def "extractWork: returns an already-linked work without creating or updating anything"() {
        given:
        def phys = ['@id'       : 'http://x/phys#it', '@type': 'Instance',
                    'instanceOf': ['@id': 'http://x/existingwork#it']]
        def xl = new FakeXL([(PHYS): phys])

        when:
        new ReproductionService(xl).createDigitalReproduction(electronic(), true)

        then: 'only the reproduction is created; no work extraction, no instance update'
        xl.created.size() == 1
        xl.created[0].thing.instanceOf == ['@id': 'http://x/existingwork#it']
        xl.updated.isEmpty()
    }

    // --- fixtures ---

    static Map electronic() {
        [
                '@type'         : 'Electronic',
                'reproductionOf': ['@id': 'http://x/phys#it'],
                'production'    : [['@type': 'Reproduction', 'date': '2021']],
        ]
    }

    static Map onlineElectronic() {
        electronic() + [
                'hasRepresentation'   : ['@id': 'http://rep'],
                'usageAndAccessPolicy': [['@id': FREELY_AVAILABLE]],
        ]
    }

    static Map physicalInstance() {
        ['@id'       : 'http://x/phys#it', '@type': 'Instance',
         'hasTitle'  : [['@type': 'Title', 'mainTitle': 'T']],
         'instanceOf': ['@id': 'http://x/work#it']]
    }

    static Map library() {
        ['@id': "$LIB_S#it"]
    }

    /**
     * Fake XL: serves canned mainEntities keyed by id (minus fragment), and records the payloads
     * passed to create()/update() instead of making HTTP calls.
     */
    static class FakeXL extends XL {
        Map records
        List created = []
        List updated = []

        FakeXL(Map records) {
            super([:], 'http://fake/')
            this.records = records
        }

        @Override
        Optional<XL.Doc> get(String id) {
            def entity = records[id.split('#')[0]]
            return entity == null
                    ? Optional.empty()
                    : Optional.of(new XL.Doc(['@graph': [[:], deepCopy(entity)]], 'etag'))
        }

        @Override
        String create(Map record, Map thing) {
            created << [record: deepCopy(record), thing: deepCopy(thing)]
            return "http://created/${created.size()}#it"
        }

        @Override
        void update(XL.Doc doc) {
            updated << deepCopy(doc.data())
        }
    }

    static Object deepCopy(Object o) {
        if (o instanceof Map) {
            return o.collectEntries { k, v -> [(k): deepCopy(v)] }
        }
        if (o instanceof List) {
            return o.collect { deepCopy(it) }
        }
        return o
    }
}
