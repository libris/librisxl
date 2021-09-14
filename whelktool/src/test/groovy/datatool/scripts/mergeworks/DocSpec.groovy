package datatool.scripts.mergeworks
import spock.lang.Specification
import whelk.util.Unicode

class DocSpec extends Specification {

    def "parse extent"() {
        expect:
        Doc.numPages(extent) == pages
        where:
        extent                                    | pages
        ""                                        | -1
        "114, [1] s."                             | 114
        "[4], 105, [2] s."                        | 105
        "21 s., ([4], 21, [5] s.)"                | 21
        "[108] s., (Ca 110 s.)"                   | 110
        "80 s., (80, [3] s., [8] pl.-bl. i färg)" | 80
        "622, [8] s."                             | 622
        "[2] s., s. 635-919, [7] s."              | 919 // ??
        "[1], iv, 295 s."                         | 295
        "3 vol."                                  | -1
        //"249, (1) s."                             | 249
        //"[8] s., s. 11-370"                       | 370
        //[12] s., s. 15-256                        | 256
        "25 onumrerade sidor"                     | 25
    }
}
