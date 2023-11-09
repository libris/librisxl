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
}
