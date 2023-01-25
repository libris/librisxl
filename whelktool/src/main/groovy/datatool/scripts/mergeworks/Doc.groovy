package datatool.scripts.mergeworks

import se.kb.libris.Normalizers
import whelk.Document
import whelk.Whelk

import static datatool.scripts.mergeworks.Util.asList

class Doc {
    public static final String SAOGF_SKÖN = 'https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur'
    public static final List MARC_FICTION = [
            'https://id.kb.se/marc/FictionNotFurtherSpecified',
            'https://id.kb.se/marc/Drama',
            'https://id.kb.se/marc/Essay',
            'https://id.kb.se/marc/Novel',
            'https://id.kb.se/marc/HumorSatiresEtc',
            'https://id.kb.se/marc/Letter',
            'https://id.kb.se/marc/ShortStory',
            'https://id.kb.se/marc/MixedForms',
            'https://id.kb.se/marc/Poetry',
    ]
    public static final List MARC_NOT_FICTION = [
            'https://id.kb.se/marc/NotFictionNotFurtherSpecified',
            'https://id.kb.se/marc/Biography'
    ]
    public static final List DRAMA_GF = [
            'https://id.kb.se/term/saogf/Dramatik',
            'https://id.kb.se/marc/Drama'
    ]

    Whelk whelk
    Document doc
    Map work
    
    List<String> titles

    //FIXME
    Document ogDoc

    DisplayDoc display
    
    Doc(Whelk whelk, Document doc) {
        this.whelk = whelk
        this.doc = doc
        this.ogDoc = doc.clone()
    }

    Map getWork() {
        if (!work) {
            work = getWork(whelk, doc)
        }

        return work
    }
    
    DisplayDoc getView() {
        if (!display) {
            display = new DisplayDoc(this)
        }

        return display
    }

    static Map getWork(Whelk whelk, Document d) {
        Map work = Normalizers.getWork(whelk, d)
        if (!work) {
            throw new NoWorkException(d.shortId)
        }
        work = new HashMap<>(work)

        //TODO 'marc:fieldref'

//        work.remove('@id')
        return work
    }

    Map workCopy() {
        return getWork(whelk, doc.clone())
    }

    String workIri() {
        getWork()['@id']
    }

    Map getMainEntity() {
        return doc.data['@graph'][1]
    }

    boolean isInstance() {
        return getMainEntity().containsKey('instanceOf')
    }

    List<String> getTitleVariants() {
        if (!titles) {
            titles = Util.getTitleVariants(getMainEntity()['hasTitle'])
        }

        return titles
    }

    boolean hasGenericTitle() {
        Util.hasGenericTitle(getMainEntity()['hasTitle'])
    }
    
    boolean isMonograph() {
        getMainEntity()['issuanceType'] == 'Monograph'
    }

    boolean hasPart() {
        getWork()['hasPart'] != null
    }

    String encodingLevel() {
        return doc.data['@graph'][0]['encodingLevel'] ?: ''
    }

    int numPages() {
        String extent = Util.getPathSafe(getMainEntity(), ['extent', 0, 'label', 0]) ?: Util.getPathSafe(getMainEntity(), ['extent', 0, 'label'], '')
        return numPages(extent)
    }

    // TODO: improve parsing https://metadatabyran.kb.se/beskrivning/materialtyper-arbetsfloden/tryckta-monografier/omfang-for-tryckta-monografier
    static int numPages(String extentLabel) {
        def l = extentLabel.replace('onumrerade', '')
        def matcher = l =~ /(\d+)(?=[, \[\]0-9]*[sp])/
        List<Integer> pages = []
        while (matcher.find()) {
            pages << Integer.parseInt(matcher.group(1))
        }
        pages ? pages.max() : -1
    }
    
    boolean isFiction() {
        isMarcFiction() || isSaogfFiction() || isSabFiction()
    }

    boolean isMarcFiction() {
        (getWork()['genreForm'] ?: []).any { it['@id'] in MARC_FICTION }
    }

    boolean isMarcNotFiction() {
        (getWork()['genreForm'] ?: []).any { it['@id'] in MARC_NOT_FICTION }
    }

    boolean isSaogfFiction() {
        (getWork()['genreForm'] ?: []).any { whelk.relations.isImpliedBy(SAOGF_SKÖN, it['@id'] ?: '') }
    }

    boolean isSabFiction() {
        view.classificationStrings().any { it.contains('kssb') && it.contains(': H') }
    }

    boolean isNotFiction() {
        // A lot of fiction has marc/NotFictionNotFurtherSpecified but then classification is usually empty
        isMarcNotFiction() && (!view.classificationStrings().isEmpty() && !isSabFiction())
    }

    boolean isText() {
        getWork()['@type'] == 'Text'
    }

    boolean isTranslationWithoutTranslator() {
        isTranslation() && !hasTranslator()
    }

    boolean isTranslation() {
        getWork()['translationOf']
    }

    boolean isSabDrama() {
        view.classificationStrings().any { it.contains(': Hc.02') || it.contains(': Hce.02') }
    }

    boolean isGfDrama() {
        asList(getWork()['genreForm']).any { it['@id'] in DRAMA_GF }
    }

    boolean isDrama() {
        isSabDrama() || isGfDrama()
    }

    boolean hasRole(String relatorIri) {
        asList(getWork()['contribution']).any {
            asList(it['role']).contains(['@id': relatorIri])
        }
    }

    boolean hasTranslator() {
        hasRole('https://id.kb.se/relator/translator')
    }

    boolean hasDistinguishingEdition() {
        (getMainEntity()['editionStatement'] ?: '').toString().toLowerCase().contains("förk")
    }

    boolean hasRelationshipWithContribution() {
        asList(getWork()['relationship']).any { r ->
            asList(r['entity']).any { e ->
                e.containsKey('contribution')
            }
        }
    }

    void addComparisonProps() {
        if (hasDistinguishingEdition()) {
            addToWork('editionStatement')
        }
        getWork()['_numPages'] = numPages()
    }

    void addToWork(String field) {
        getWork()[field] = getMainEntity()[field]
    }

    void removeComparisonProps() {
        getWork().remove('editionStatement')
        getWork().remove('_numPages')
    }
}