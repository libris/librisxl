package datatool.scripts.mergeworks

class Util {
    static List asList(Object o) {
        (o ?: []).with { it instanceof List ? it : [it] }
    }
}
