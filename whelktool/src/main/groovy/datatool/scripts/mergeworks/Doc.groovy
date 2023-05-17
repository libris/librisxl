package datatool.scripts.mergeworks


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
    Document document

    Map instanceData
    Map workData

    List<String> flatInstanceTitle

    DisplayDoc display

    String checksum

    Doc(Whelk whelk, Document document) {
        this.whelk = whelk
        this.document = document
        this.checksum = document.getChecksum(whelk.getJsonld())
        setData()
    }

    void setData() {
        if (mainEntity()['instanceOf']) {
            instanceData = mainEntity()
            workData = instanceData['instanceOf']
        } else {
            workData = mainEntity()
        }
    }

    DisplayDoc getView() {
        if (!display) {
            display = new DisplayDoc(this)
        }

        return display
    }

    Map mainEntity() {
        return document.data['@graph'][1]
    }

    String encodingLevel() {
        return document.data['@graph'][0]['encodingLevel'] ?: ''
    }

    String workIri() {
        workData['@id']
    }

    List<Map> workTitle() {
        asList(workData['hasTitle'])
    }

    List<Map> instanceTitle() {
        asList(instanceData?.hasTitle)
    }

    List<String> flatInstanceTitle() {
        if (!flatInstanceTitle) {
            flatInstanceTitle = Util.getFlatTitle(instanceTitle())
        }

        return flatInstanceTitle
    }

    boolean hasGenericTitle() {
        Util.hasGenericTitle(instanceTitle())
    }

    String workType() {
        workData['@type']
    }

    String instanceType() {
        instanceData?.'@type'
    }

    List<Map> translationOf() {
        asList(workData['translationOf'])
    }

    List<Map> contribution() {
        asList(workData['contribution'])
    }

    List<Map> classification() {
        asList(workData['classification'])
    }

    List<Map> genreForm() {
        asList(workData['genreForm'])
    }

    List<Map> publication() {
        asList(instanceData?.publication)
    }

    List<Map> identifiedBy() {
        asList(instanceData?.identifiedBy)
    }

    List<Map> extent() {
        asList(instanceData?.extent)
    }

    List<Map> reproductionOf() {
        asList(instanceData?.reproductionOf)
    }

    String editionStatement() {
        instanceData?.editionStatement
    }

    String responsibilityStatement() {
        instanceData?.responsibilityStatement
    }

    boolean isMonograph() {
        instanceData?.issuanceType == 'Monograph'
    }

    boolean hasPart() {
        workData['hasPart'] != null
    }

    int numPages() {
        String extent = Util.getPathSafe(extent(), [0, 'label', 0]) ?: Util.getPathSafe(extent(), [0, 'label'], '')
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
        (genreForm() ?: []).any { it['@id'] in MARC_FICTION }
    }

    boolean isMarcNotFiction() {
        (genreForm() ?: []).any { it['@id'] in MARC_NOT_FICTION }
    }

    boolean isSaogfFiction() {
        (genreForm() ?: []).any { whelk.relations.isImpliedBy(SAOGF_SKÖN, it['@id'] ?: '') }
    }

    boolean isSabFiction() {
        getView().classificationStrings().any { it.contains('kssb') && it.contains(': H') }
    }

    boolean isNotFiction() {
        // A lot of fiction has marc/NotFictionNotFurtherSpecified but then classification is usually empty
        isMarcNotFiction() && (!getView().classificationStrings().isEmpty() && !isSabFiction())
    }

    boolean isText() {
        workData['@type'] == 'Text'
    }

    boolean isTranslationWithoutTranslator() {
        translationOf() && !hasTranslator()
    }

    boolean hasTranslator() {
        hasRole(Util.Relator.TRANSLATOR.iri)
    }

    boolean hasRole(String relatorIri) {
        asList(workData['contribution']).any {
            asList(it['role']).contains(['@id': relatorIri])
        }
    }

    boolean isDrama() {
        isSabDrama() || isGfDrama()
    }

    boolean isSabDrama() {
        getView().classificationStrings().any { it.contains(': Hc.02') || it.contains(': Hce.02') }
    }

    boolean isGfDrama() {
        asList(genreForm()).any { it['@id'] in DRAMA_GF }
    }

    boolean hasRelationshipWithContribution() {
        asList(workData['relationship']).any { r ->
            asList(r['entity']).any { e ->
                e.containsKey('contribution')
            }
        }
    }

    boolean isTactile() {
        asList(workData['contentType']).contains(['@id': 'https://id.kb.se/term/rda/TactileText'])
    }

    boolean hasDistinguishingEdition() {
        (instanceData?.editionStatement ?: '').toString().toLowerCase().contains("förk")
    }

    void addComparisonProps() {
        if (hasDistinguishingEdition()) {
            workData['_editionStatement'] = instanceData['editionStatement']
        }
        workData['_numPages'] = numPages()
    }

    void removeComparisonProps() {
        workData.remove('_editionStatement')
        workData.remove('_numPages')
    }
}