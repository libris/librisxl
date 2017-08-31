/**
 * Created by theodortolstoy on 2017-04-19.
 */

import spock.lang.Specification
import whelk.AuthBibMatcher

class AuthBibMatcherSpec extends Specification {

    void setup() {}

    def "Has valid authRecords"() {
        when:
        def authRecords = [
                ['bibid': 2, 'id': 40578, 'data': ['leader': '00229cz  a2200097n  4500', 'fields': [['001': '40578'], ['005': '20061002101703.0'], ['008': '011206n| ac|nnaabn          |n aad     d'], ['035': ['ind1': ' ', 'ind2': ' ', 'subfields': [['a': '(LIBRIS)A000009532']]]], ['100': ['ind1': '1', 'ind2': ' ', 'subfields': [['a': 'Bertilsson, Bertil']]]], ['400': ['ind1': '1', 'ind2': ' ', 'subfields': [['a': 'Mögholm, Figge']]]]]]],
                ['bibid': 2, 'id': 65233, 'data': ['leader': '00259cz  a2200097n  4500', 'fields': [['001': '65233'], ['005': '20061002110145.0'], ['008': '011206n| ac|nnaabn          |n aad     d'], ['035': ['ind1': ' ', 'ind2': ' ', 'subfields': [['a': '(LIBRIS)A000034187']]]], ['100': ['ind1': '1', 'ind2': ' ', 'subfields': [['a': 'Jossehonanon, Ismael']]]], ['500': ['ind1': '1', 'ind2': ' ', 'subfields': [['w': 'i'], ['i': 'Folkbiblioteksform:'], ['a': 'Jösseligen, Ismael']]]]]]]
        ]
        then:
        true

    }





}
