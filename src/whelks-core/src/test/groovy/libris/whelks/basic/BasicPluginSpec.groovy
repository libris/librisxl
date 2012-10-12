package se.kb.libris.whelks.basic

import se.kb.libris.whelks.plugin.*

import spock.lang.Specification

class BasicPluginSpec extends Specification {

    SortedSet ss 

    def "should be properly sorted"() {
        expect:
        ss.collect { it.id } == order
        where:
        ss                                                                                                        | order
        new TreeSet([new TestBasicPlugin("p1", 1), new TestBasicPlugin("p2", 2), new TestBasicPlugin("p3", 3)])   | ["p1", "p2", "p3" ]
        new TreeSet([new TestBasicPlugin("p1", 100), new TestBasicPlugin("p2", 2), new TestBasicPlugin("p3", 3)]) | ["p2", "p3", "p1" ]
        new TreeSet([new TestBasicPlugin("p1", 1), new TestBasicPlugin("p2", 5), new TestBasicPlugin("p3", 2)])   | ["p1", "p3", "p2" ]
        new TreeSet([new TestBasicPlugin("p1", 1), new TestBasicPlugin("p2", 2), new TestBasicPlugin("p3", 0)])   | ["p3", "p1", "p2" ]
        new TreeSet([new TestBasicPlugin("p1", 0), new TestBasicPlugin("p2", 0), new TestBasicPlugin("p3", 0)])   | ["p1", "p2", "p3" ]
    }
}

class TestBasicPlugin extends BasicPlugin {
    String id
    TestBasicPlugin(id, order) {
        this.id = id
        this.order = order
    }
}

