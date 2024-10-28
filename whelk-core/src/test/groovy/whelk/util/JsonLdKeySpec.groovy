package whelk.util

import spock.lang.Specification

class JsonLdKeySpec extends Specification {
    enum TestEnum implements JsonLdKey {

        A("a"),
        B("b")

        private String key;

        TestEnum(String key) {
            this.key = key
        }

        @Override
        String key() {
            return key
        }
    }

    def "fromKey"() {
        expect:
        JsonLdKey.fromKey(TestEnum.class, key) == result
        where:
        key     | result
        null    | null
        "a"     | TestEnum.A
        "b"     | TestEnum.B
        "x"     | null
    }
}
