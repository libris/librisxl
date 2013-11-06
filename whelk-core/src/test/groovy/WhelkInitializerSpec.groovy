package se.kb.libris.whelks

import spock.lang.*

class WhelkInitializerSpec extends Specification {

    @Shared wi = new WhelkInitializer(
            new ByteArrayInputStream("""{
                "_whelks": [
                    {
                        "default": {
                            "_class" : "se.kb.libris.whelks.StandardWhelk",
                            "_plugins": ["basic1", "basic2"]
                        }
                    }
                ]
            }""".bytes),
            new ByteArrayInputStream("""{
                "basic1": {
                    "_class" : "se.kb.libris.whelks.plugin.BasicPlugin"
                },
                "basic2": {
                    "_class" : "se.kb.libris.whelks.plugin.BasicPlugin"
                }
            }""".bytes))

    @Shared whelks

    def setupSpec() {
        whelks = wi.getWhelks() // force reading of whelks...
    }

    def "should get plugins"() {
        when:
        def plugin = wi.getPlugin(name, "default")
        then:
        name == plugin.id
        where:
        name << ["basic1", "basic2"]
    }

    def "should expand parameter variables"() {
        given:
        def params = wi.translateParams("_whelk:default, other", "default")
        expect:
        params == [whelks[0], "other"]
    }

}
