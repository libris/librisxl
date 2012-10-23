package se.kb.libris.whelks.component

import spock.lang.*
import se.kb.libris.whelks.basic.BasicDocument
import java.net.URI

class RiakStorageSpec extends Specification {

    def rs = new RiakStorage("bib")
    def doc = new BasicDocument().withIdentifier("/bib/777").withData("test").withContentType("text/plain")
    
    def "should store document in riak"(){
        when:
        rs.store(doc)

        then:
        notThrown(Exception)
    }

    def "should extract id from uri"(){
        given:
        URI uri = new URI("/bib/777")

        expect:
        rs.extractIdFromURI(uri) == "777"
    }


    def "should get document from riak"(){
        given:
        URI uri = new URI("/bib/777")

        expect:
        rs.get(uri) != null
    }

    def "should delete document from riak"(){
        when:
        rs.delete(new URI("/bib/777"))

        then:
        notThrown(Exception)
    }
}

