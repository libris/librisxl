package se.kb.libris.whelks.basic

import se.kb.libris.whelks.plugin.*

import spock.lang.Specification

class BasicOrderedPluginSpec extends Specification {


    def "should be properly sorted"() {
        given:
        def p1 = new BasicOrderedPlugin()
        p1.id = "p1"
        def p2 = new BasicOrderedPlugin()
        p2.id = "p2"
        def p3 = new BasicOrderedPlugin()
        p3.id = "p3"
        SortedSet ss = new SortedSet()
        when:
        p1.order = 3
        p2.order = 2
        p3.order = 1
        ss.add(p1)
        ss.add(p2)
        ss.add(p3)
        then:
        "p3p2p1" == ss.collect { it }
    }

}

