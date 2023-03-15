package whelk.converter.marc

import whelk.filter.LanguageLinker
import static whelk.JsonLd.asList
import static whelk.JsonLd.ID_KEY as ID
import static whelk.JsonLd.TYPE_KEY as TYPE

class NormalizeWorkTitlesStep extends MarcFramePostProcStepBase {

    private static final String EXPRESSION_OF = 'expressionOf'
    private static final String TRANSLATION_OF = 'translationOf'
    private static final String LANGUAGE = 'language'
    private static final String HAS_TITLE = 'hasTitle'
    private static final String TITLE = 'Title'
    private static final String LEGAL_DATE = 'legalDate'
    private static final String CONTRIBUTION = 'contribution'

    private static final List<String> titleProps = [HAS_TITLE, 'musicKey', 'musicMedium', 'version', LEGAL_DATE, 'originDate', 'marc:arrangedStatementForMusic']

    LanguageLinker langLinker

    void modify(Map record, Map thing) {
//        traverse(thing, null, true)
    }

    void unmodify(Map record, Map thing) {
        traverse(thing)
    }

    void traverse(o, String via = null, convert = false) {
        asList(o).each {
            if (it instanceof Map) {
                if (convert) {
                    shuffleTitles(it)
                } else if (!('_revertOnly' in it)) {
                    useOriginalTitle(it, via)
                }
                it.each { k, v ->
                    traverse(v, k, convert)
                }
            }
        }
    }

    void useOriginalTitle(Map work, String via = null) {
        asList(work[TRANSLATION_OF]).each { original ->
            def originalTitle = asList(original[HAS_TITLE]).find { it[TYPE] == TITLE }

            Set workLangIds = asList(work[LANGUAGE]).collect { it[ID] }
            if (originalTitle instanceof Map) {
                boolean noWorkLangOrNotSameLang =
                        workLangIds.size() == 0
                                || !original[LANGUAGE]
                                || !asList(original[LANGUAGE]).every { it[ID] in workLangIds }
                if (noWorkLangOrNotSameLang) {
                    markToIgnore(work[HAS_TITLE])
                    if (!work[HAS_TITLE]) work[HAS_TITLE] = []
                    work[HAS_TITLE] << copyForRevert(originalTitle)
                    original.remove(HAS_TITLE)
                    if (original[LEGAL_DATE] && !work[LEGAL_DATE]) {
                        work[LEGAL_DATE] = original[LEGAL_DATE]
                    }
                }
            }
        }

        if (via == 'instanceOf'
                && !work[CONTRIBUTION]?.find { it[TYPE] == 'PrimaryContribution' }
                && !work[EXPRESSION_OF]
                && work[HAS_TITLE]
        ) {
            def workTitle = work[HAS_TITLE]?.find { it[TYPE] == TITLE && !('_revertedBy' in it) }
            if (workTitle instanceof Map) {
                def copiedTitle = copyForRevert(workTitle)
                markToIgnore(workTitle)
                work[EXPRESSION_OF] = [
                        (TYPE)     : 'Work',
                        (HAS_TITLE)   : [copiedTitle],
                        _revertOnly: true
                ]
                (titleProps - HAS_TITLE).each {
                    if (work[it]) {
                        work[EXPRESSION_OF][it] = work[it]
                    }
                }
                addLangFor130(work)
            }
        }
    }

    void markToIgnore(o) {
        asList(o).each {
            it._revertedBy = 'NormalizeWorkTitlesStep'
        }
    }

    Map copyForRevert(Map item) {
        item = item.clone()
        item._revertOnly = true
        return item
    }

    void addLangFor130(Map work) {
        List langs = work[TRANSLATION_OF] ? asList(work[TRANSLATION_OF]).findResults { it[LANGUAGE] }.flatten() : work[LANGUAGE]
        if (langs) {
            work[EXPRESSION_OF][LANGUAGE] = langs[0..<1]
        }
    }

    //TODO: Implement here or in marcframe?
    void normalizePunctuationOnRevert(Object title) {

    }

    void normalizePunctuationOnConvert(Object title) {

    }

    void shuffleTitles(Map work) {
        if (hasOkExpressionOf(work)) {
            // try normalize title according to curated lists?
            if (tryMove(work[EXPRESSION_OF], work, titleProps)) {
                work.remove(EXPRESSION_OF)
            }
        }

        def isMusic = work[TYPE] in ['Music', 'NotatedMusic']
        def hasTranslator = asList(work[CONTRIBUTION]).any { Map c ->
            asList(c.role).any { Map r ->
                r.code == 'trl' || r[ID] == 'https://id.kb.se/relator/translator'
            }
        }

        if (!isMusic) {
            if (!work[TRANSLATION_OF] && hasTranslator) {
                work[TRANSLATION_OF] = [(TYPE): 'Work']
            }
            if (asList(work[TRANSLATION_OF]).size() == 1) {
                tryMove(work, work[TRANSLATION_OF], [HAS_TITLE, LEGAL_DATE])
            }
        }
    }

    boolean tryMove(Object from, Object target, Collection properties) {
        from = asList(from)[0]
        target = asList(target)[0]

        def moveThese = properties ? from.keySet().intersect(properties) : from.keySet()
        def conflictingProps = moveThese.intersect(target.keySet())

        if (conflictingProps && conflictingProps.any { from[it] != target[it] }) {
            return false
        }

        from.removeAll { k, v ->
            if (k in moveThese) {
                target[k] = v
                return true
            }
            return false
        }

        return true
    }

    boolean hasOkExpressionOf(Map work) {
        if (asList(work[EXPRESSION_OF]).size() != 1) {
            return false
        }
        def expressionOf = asList(work[EXPRESSION_OF])[0]
        if (!expressionOf[HAS_TITLE]) {
            return false
        }
        langLinker.linkAll(work)
        def workLang = asList(work[LANGUAGE])
        def trlOf = asList(work[TRANSLATION_OF])[0]
        def trlOfLang = trlOf ? asList(trlOf[LANGUAGE]) : []
        langLinker.linkLanguages(expressionOf, workLang + trlOfLang)
        def exprOfLang = asList(expressionOf[LANGUAGE])
        return (workLang + trlOfLang).containsAll(exprOfLang)
    }
}
