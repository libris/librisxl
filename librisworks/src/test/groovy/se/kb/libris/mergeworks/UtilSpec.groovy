package se.kb.libris.mergeworks

import spock.lang.Specification
import whelk.Document
import whelk.Whelk

class UtilSpec extends Specification {
    static def whelk = null
    static {
        try {
            whelk = Whelk.createLoadedSearchWhelk()
        } catch (Exception e) {
            System.err.println("Unable to instantiate whelk: $e")
        }
    }

    def "generic mainTitle"() {
        expect:
        Util.hasGenericTitle([['mainTitle': mainTitle]]) == result

        where:
        mainTitle                  || result
        'dikter'                   || true
        'Samlade verk'             || true
        'Tusen och en natt'        || true
        'en lite ovanligare titel' || false
    }

    def "drop generic subtitle"() {
        given:
        def hasTitle = [['mainTitle': 'x', 'subtitle': subtitle]]
        def dropped = Util.dropGenericSubTitles(hasTitle)

        expect:
        dropped[0]['subtitle'] == result && hasTitle == [['mainTitle': 'x', 'subtitle': subtitle]]

        where:
        subtitle                           || result
        'en äktenskapshistoria'            || null
        'avhandlingar'                     || null
        'En Roland Hassel-thriller'        || null
        'En lite ovanligare titel : Roman' || 'En lite ovanligare titel'
        'En lite ovanligare titel'         || 'En lite ovanligare titel'
    }

    def "flatten titles"() {
        expect:
        Util.flatTitles(title)[0]['flatTitle'] == result

        where:
        title                                                                       || result
        [['mainTitle': 'x']]                                                        || 'x'
        [['mainTitle': 'x', 'subtitle': 'y']]                                       || 'x y'
        [['mainTitle': 'x', 'subtitle': 'roman']]                                   || 'x'
        [['mainTitle': 'x', 'hasPart': ['partNumber': '1', 'partName': 'y']]]       || 'x 1 y'
        [['mainTitle': 'x-y.', 'hasPart': ['partNumber': '[1]'], 'subtitle': 'é ']] || 'x y e 1'
    }

    def "find title parts"() {
        expect:
        Util.findTitlePart(title, 'partNumber') == partNumber
        Util.findTitlePart(title, 'partName') == partName

        where:
        title                                                   || partNumber || partName
        [['hasPart': [['partNumber': '1', 'partName': ['x']]]]] || '1'        || 'x'
        [['hasPart': [['partNumber': '1']]]]                    || '1'        || null
        [['hasPart': [['partName': 'x']]]]                      || null       || 'x'
        [['partNumber': '1', 'partName': 'x']]                  || '1'        || 'x'
        [['mainTitle': 'x']]                                    || null       || null
    }

    def "append title parts to main title"() {
        given:
        Util.appendTitlePartsToMainTitle(title, partNumber, partName)

        expect:
        title == result

        where:
        title               || partNumber || partName || result
        ['mainTitle': 'x']  || '1'        || 'y'      || ['mainTitle': 'x. 1, y']
        ['mainTitle': 'x.'] || '1'        || null     || ['mainTitle': 'x. 1']
        ['mainTitle': 'x.'] || null       || 'y'      || ['mainTitle': 'x. y']
        ['mainTitle': 'x.'] || null       || null     || ['mainTitle': 'x.']
    }

    def "pick best work title"() {
        def fl = 'marc:FullLevel'
        def ml = 'marc:MinimalLevel'

        def createDoc = { tuple, i ->
            def (instanceTitle, encodingLevel) = tuple

            def data = [
                    '@graph': [
                            [
                                    'encodingLevel': encodingLevel
                            ],
                            [
                                    '@id'       : "https://libris.kb.se/x$i",
                                    'hasTitle'  : [['@type': 'Title', 'mainTitle': instanceTitle]],
                                    'instanceOf': ['@type': 'Text']
                            ]
                    ]
            ]
            return new Doc(whelk, new Document(data))
        }

        def collectDocs = { instanceTitles, encodingLevels ->
            [instanceTitles, encodingLevels].transpose().withIndex().collect(createDoc)
        }

        // Same encoding level, no work titles --> pick most common instance title
        when:
        def instanceTitles1 = ['T', 'T.', 't', 't']
        def encodingLevels1 = [fl, fl, fl, fl]
        def docs1 = collectDocs([instanceTitles1, encodingLevels1])

        then:
        Util.bestTitle(docs1) == [['@type': 'Title', 'mainTitle': 't', 'source': [['@id': 'https://libris.kb.se/x3']]]]

        // Different encoding levels, no work titles --> pick most common instance title among docs with highest level
        when:
        def instanceTitles2 = ['T', 'T', 't', 't', 't']
        def encodingLevels2 = [fl, fl, fl, ml, ml]
        def docs2 = collectDocs([instanceTitles2, encodingLevels2])

        then:
        Util.bestTitle(docs2) == [['@type': 'Title', 'mainTitle': 'T', 'source': [['@id': 'https://libris.kb.se/x1']]]]

        // Pick existing work title over instance titles
        when:
        def instanceTitles3 = ['t', 't', 't', 't']
        def encodingLevels3 = [ml, fl, ml, fl]
        def docs3 = collectDocs([instanceTitles3, encodingLevels3])
        docs3[0].workData['hasTitle'] = [['@type': 'Title', 'mainTitle': 'T']]

        then:
        Util.bestTitle(docs3) == [['@type': 'Title', 'mainTitle': 'T']]

        // Pick from linkable work over local works
        when:
        def instanceTitles4 = ['t', 't', 't']
        def encodingLevels4 = [null, fl, fl]
        def docs4 = collectDocs([instanceTitles4, encodingLevels4])
        docs4[0].workData['hasTitle'] = [['@type': 'Title', 'mainTitle': 'T']]
        docs4[0].workData['@id'] = 'https://libris.kb.se/y'
        docs4[1].workData['hasTitle'] = [['@type': 'Title', 'mainTitle': 't.']]
        docs4[2].workData['hasTitle'] = [['@type': 'Title', 'mainTitle': 't.']]

        then:
        Util.bestTitle(docs4) == [['@type': 'Title', 'mainTitle': 'T']]

        // Ignore generic subtitles
        when:
        def instanceTitles5 = ['t', 't', 't', 'T', 'T']
        def encodingLevels5 = [fl, fl, fl, fl, fl]
        def docs5 = collectDocs([instanceTitles5, encodingLevels5])
        docs5[0].instanceTitle()[0]['subtitle'] = 'roman'
        docs5[1].instanceTitle()[0]['subtitle'] = 'en roman'

        then:
        Util.bestTitle(docs5) == [['@type': 'Title', 'mainTitle': 't', 'source': [['@id': 'https://libris.kb.se/x2']]]]

        // Don't ignore any subtitle
        when:
        def instanceTitles6 = ['t', 'T', 'T']
        def encodingLevels6 = [fl, fl, fl]
        def docs6 = collectDocs([instanceTitles6, encodingLevels6]).each {
            it.instanceTitle()[0]['subtitle'] = 'en lite ovanligare titel'
        }

        then:
        Util.bestTitle(docs6) == [['@type': 'Title', 'mainTitle': 'T', 'subtitle': 'en lite ovanligare titel', 'source': [['@id': 'https://libris.kb.se/x2']]]]

        // Append parts to mainTitle
        when:
        def instanceTitles7 = ['T', 'T', 'T']
        def encodingLevels7 = [fl, fl, fl]
        def docs7 = collectDocs([instanceTitles7, encodingLevels7]).each {
            it.instanceTitle()[0]['hasPart'] = [['partNumber': '1', 'partName': 'Delens titel']]
        }

        then:
        Util.bestTitle(docs7) == [['@type': 'Title', 'mainTitle': 'T. 1, Delens titel', 'source': [['@id': 'https://libris.kb.se/x2']]]]

        // Append only partNumber to existing work mainTitle
        when:
        def instanceTitles8 = ['t', 't', 't']
        def encodingLevels8 = [fl, fl, fl]
        def docs8 = collectDocs([instanceTitles8, encodingLevels8]).each {
            it.instanceTitle()[0]['hasPart'] = [['partNumber': '1', 'partName': 'Delens titel']]
        }
        docs8[0].workData['hasTitle'] = [['@type': 'Title', 'mainTitle': 'T.']]

        then:
        Util.bestTitle(docs8) == [['@type': 'Title', 'mainTitle': 'T. 1']]
    }

    def "pick best original title"() {
        given:
        def fl = 'marc:FullLevel'
        def ml = 'marc:MinimalLevel'
        def origTitles = ['t', 't', 'T', 'T', 'T.', 'T.']
        def encodingLevels = [fl, ml, fl, fl, ml, fl]
        def docs = [origTitles, encodingLevels].transpose().collect { origTitle, encodingLevel ->
            def data = [
                    '@graph': [
                            [
                                    'encodingLevel': encodingLevel
                            ],
                            [
                                    'instanceOf': ['translationOf': ['hasTitle': [['@type': 'Title', 'mainTitle': origTitle]]]]
                            ]
                    ]
            ]
            return new Doc(whelk, new Document(data))
        }

        expect:
        Util.bestOriginalTitle(docs) == [['@type': 'Title', 'mainTitle': 'T']]
    }
}
