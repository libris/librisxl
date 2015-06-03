package whelk

import spock.lang.Specification

class StandardWhelkSpec extends Specification {

    def whelk

    def setup() {
        whelk = new StandardWhelk() {
            def camelStatus = [:]

            void notifyCamel(String identifier, String dataset, String operation, Map extraInfo) {
                this.camelStatus['identifier'] = identifier
                this.camelStatus['dataset'] = dataset
                this.camelStatus['operation'] = operation
            }
            void notifyCamel(Document document, String operation, Map extraInfo) {
                this.camelStatus['identifier'] = document.identifier
                this.camelStatus['dataset'] = document.dataset
                this.camelStatus['operation'] = operation
            }
            Location locate(String id) {
                if (id == "/record/1234") {
                    return new Location().withURI("/record/xxx-aaaa-111-zzz").withResponseCode(301)
                }
                return new Location(new JsonLdDocument().withIdentifier(id).withData(["@id":id]).withDataset("record"))
            }
        }
    }

    def "should send correct identifier to DELETE"() {
        given:
        def id = "/record/2345"
        when:
        whelk.remove(id)
        then:
        whelk.camelStatus['identifier'] == id
        whelk.camelStatus['dataset'] == "record"
        whelk.camelStatus['operation'] == Whelk.REMOVE_OPERATION
    }

    def "should send correct identifier to DELETE for alternate identifier request"() {
        given:
        def id = "/record/1234"
        when:
        whelk.remove(id, "datasetA")
        then:
        whelk.camelStatus['identifier'] == "/record/xxx-aaaa-111-zzz"
        whelk.camelStatus['dataset'] == "datasetA"
        whelk.camelStatus['operation'] == Whelk.REMOVE_OPERATION
    }
}
