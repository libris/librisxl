package se.kb.libris.conch

import org.junit.Test

class WhelksTest {

    @Test
    void test_create_random_URI() {
        println "testing uri"
        Whelk w = new Whelk("foo")
        def uri = w._create_random_URI()
        assert uri instanceof java.net.URI
    }
}
