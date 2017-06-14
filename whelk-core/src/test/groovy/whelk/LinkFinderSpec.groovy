package whelk

import spock.lang.Ignore
import spock.lang.Specification
import whelk.filter.LinkFinder
import whelk.util.PropertyLoader


class LinkFinderSpec extends Specification {


    @Ignore
    def "All exists"() {
        setup:
        def props = PropertyLoader.loadProperties('secret')
        def pico = Whelk.getPreparedComponentsContainer(props)
        def whelk = pico.getComponent(Whelk.class)
        LinkFinder authFinder = new LinkFinder(whelk.storage)

        def entities = [[
                                "@type": "Person", "birthYear": "1914", "deathYear": "2001", "familyName": "Jansson", "givenName": "Tove"
                        ], [
                                "@type"     : "Person",
                                "birthYear" : "1926",
                                "deathYear" : "2000",
                                "familyName": "Jansson",
                                "givenName" : "Lars"

                        ]]
        def authIds = ["http://libris.kb.se/auth/191493", "http://libris.kb.se/auth/191503"]

        when:
        def results = authFinder.findLinks(entities, authIds)

        then:
        results.every { authUri -> authUri != null }
        results.size() == authIds.size()


    }
    @Ignore
    def "S1ome extra entities"() {
        setup:
        def props = PropertyLoader.loadProperties('secret')
        def pico = Whelk.getPreparedComponentsContainer(props)
        def whelk = pico.getComponent(Whelk.class)
        LinkFinder authFinder = new LinkFinder(whelk.storage)

        def entities = [[
                                "@type": "Person", "birthYear": "1914", "deathYear": "2001", "familyName": "Jansson", "givenName": "Tove"
                        ], [
                                "@type"     : "Person",
                                "birthYear" : "1926",
                                "deathYear" : "2000",
                                "familyName": "Jansson",
                                "givenName" : "Lars"

                        ],
                        [
                                "@type"     : "Person",
                                "birthYear" : "1964",
                                "familyName": "Hall",
                                "givenName" : "Marcel"

                        ]]
        def authIds = ["http://libris.kb.se/auth/191493", "http://libris.kb.se/auth/191503"]

        when:
        def results = authFinder.findLinks(entities, authIds)

        then:
        results[0] != null
        results[1] != null
        results[2] == null
        results.size() == entities.size()
        results.size() == authIds.size() + 1


    }

}


