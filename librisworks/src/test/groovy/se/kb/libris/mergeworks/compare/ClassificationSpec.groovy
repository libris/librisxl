package se.kb.libris.mergeworks.compare

import spock.lang.Specification

class ClassificationSpec extends Specification {
    def "merge SAB codes"() {
        expect:
        Classification.tryMergeSabCodes(a, b) == result

        where:
        a           || b         || result
        'H'         || 'H'       || 'H'
        'Haaa'      || 'H'       || 'Haaa'
        'Hcqaa'     || 'Hcbqbbb' || 'Hcqaa'
        'Hcb'       || 'Hc'      || null
        'Hci'       || 'Hci,u'   || 'Hci,u'
        'Hcd.016'   || 'Hcd.01'  || 'Hcd.016'
        'Hc.01'     || 'Hcd.01'  || 'Hcd.01'
        'Hda.017=c' || 'Hda.018' || 'Hda.017=c'
        'He'        || 'Hc'      || null
    }

    def "find which of multiple Dewey codes to keep in classification"() {
        given:
        def onMerged = (0..<editionsOnMerged.size()).collect { i ->
            [
                    'code'              : "x" + i,
                    'editionEnumeration': editionsOnMerged[i]
            ]
        }
        def all = allCodes.collect { ['code': it] }

        expect:
        Classification.findPreferredDewey(onMerged, all) == result

        where:
        editionsOnMerged     || allCodes                       || result
        ['23/swe', '23/swe'] || ['x0', 'x0', 'x1', 'x1', 'x1'] || ['code': 'x1', 'editionEnumeration': '23/swe']
        [null, '23/swe']     || ['x0', 'x0', 'x1', 'x1', null] || ['code': 'x1', 'editionEnumeration': '23/swe']
        ['23', '22/swe']     || ['x0', 'x0', 'x1', 'x1']       || ['code': 'x0', 'editionEnumeration': '23']
        ['23/swe', '23']     || ['x0', 'x0', 'x1', 'x1']       || ['code': 'x0', 'editionEnumeration': '23/swe']
        ['22', '23/swe']     || ['x0', 'x0', 'x0', 'x1', 'x1'] || ['code': 'x0', 'editionEnumeration': '22']
    }
}
