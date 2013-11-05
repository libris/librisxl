package se.kb.libris.whelks

import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.plugin.*

import spock.lang.Specification

class WhelkInitializerSpec extends Specification {

    def wi = new WhelkInitializer(
            new ByteArrayInputStream("""{"_whelks": [ {
                    "default": {
                        "_class" : "se.kb.libris.whelks.StandardWhelk"
                    } } ] }""".bytes),
            new ByteArrayInputStream("[]".bytes))

    def "should expand parameter variables"() {
        given:
        def whelks = wi.getWhelks() // force reading of whelks...
        def params = wi.translateParams("_whelk:default, other", "default")
        expect:
        params == [whelks[0], "other"]
    }

}
