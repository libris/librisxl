package se.kb.libris.mergeworks

import whelk.Document
import whelk.Whelk
import whelk.datatool.DocumentItem
import whelk.util.DocumentUtil

import static Util.asList
import static Util.Relator
import static Util.AGENT
import static Util.CLASSIFICATION
import static Util.CODE
import static Util.CONTENT_TYPE
import static Util.CONTRIBUTION
import static Util.EDITION_STATEMENT
import static Util.ENCODING_LEVEL
import static Util.EXTENT
import static Util.GENRE_FORM
import static Util.HAS_PART
import static Util.HAS_TITLE
import static Util.IDENTIFIED_BY
import static Util.INTENDED_AUDIENCE
import static Util.IN_SCHEME
import static Util.LABEL
import static Util.MAIN_TITLE
import static Util.PART_NAME
import static Util.PART_NUMBER
import static Util.PHYS_NOTE
import static Util.PRIMARY
import static Util.PUBLICATION
import static Util.REPRODUCTION_OF
import static Util.RESP_STATEMENT
import static Util.ROLE
import static Util.SUBTITLE
import static Util.TITLE
import static Util.TITLE_REMAINDER
import static Util.TRANSLATION_OF
import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.WORK_KEY


/**
 * Wrapper around a whelk.Document for easy access to various entities/properties
 */
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
    DocumentItem docItem
    boolean existsInStorage = true

    Map instanceData
    Map workData

    List<String> flatInstanceTitle
    List<String> flatWorkTitle

    DisplayDoc display

    Doc(DocumentItem docItem) {
        this.whelk = docItem.whelk
        this.document = docItem.doc
        this.existsInStorage = docItem.existsInStorage
        setDocItemIfNew(docItem)
        setData()
    }

    Doc(Whelk whelk, Document document) {
        this.whelk = whelk
        this.document = document
        setData()
    }

    void setDocItemIfNew(DocumentItem docItem) {
        if (!existsInStorage) {
            this.docItem = docItem
        }
    }

    void setData() {
        if (mainEntity()[WORK_KEY]) {
            instanceData = mainEntity()
            workData = asList(instanceData[WORK_KEY]).find()
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

    Map record() {
        document.data[GRAPH_KEY][0]
    }

    Map mainEntity() {
        document.data[GRAPH_KEY][1]
    }

    String shortId() {
        document.shortId
    }

    String thingIri() {
        document.getThingIdentifiers().first()
    }

    String recordIri() {
        document.getCompleteSystemId()
    }

    String encodingLevel() {
        return record()[ENCODING_LEVEL] ?: ''
    }

    String workIri() {
        workData[ID_KEY]
    }

    List<Map> workTitle() {
        asList(workData[HAS_TITLE])
    }

    List<String> flatWorkTitle() {
        if (!flatWorkTitle) {
            flatWorkTitle = Util.getFlatTitle(workTitle())
        }

        return flatWorkTitle
    }

    List<Map> instanceTitle() {
        asList(instanceData?[HAS_TITLE])
    }

    List<String> flatInstanceTitle() {
        if (!flatInstanceTitle) {
            flatInstanceTitle = Util.getFlatTitle(instanceTitle())
        }

        return flatInstanceTitle
    }

    String workType() {
        workData[TYPE_KEY]
    }

    String instanceType() {
        instanceData?[TYPE_KEY]
    }

    List<Map> translationOf() {
        asList(workData[TRANSLATION_OF])
    }

    List<Map> contribution() {
        asList(workData[CONTRIBUTION])
    }

    List<Map> classification() {
        asList(workData[CLASSIFICATION])
    }

    List<Map> genreForm() {
        asList(workData[GENRE_FORM])
    }

    List<Map> intendedAudience() {
        asList(workData[INTENDED_AUDIENCE])
    }

    List<Map> publication() {
        asList(instanceData?[PUBLICATION])
    }

    List<Map> identifiedBy() {
        asList(instanceData?[IDENTIFIED_BY])
    }

    List<Map> extent() {
        asList(instanceData?[EXTENT])
    }

    List<Map> reproductionOf() {
        asList(instanceData?[REPRODUCTION_OF])
    }

    Map primaryContributor() {
        contribution().findResult { it[TYPE_KEY] == PRIMARY ? asList(it[AGENT]).find() : null }
    }

    String editionStatement() {
        instanceData?[EDITION_STATEMENT]
    }

    String responsibilityStatement() {
        instanceData?[RESP_STATEMENT]
    }

    String physicalDetailsNote() {
        instanceData?[PHYS_NOTE]
    }

    int numPages() {
        String extent = DocumentUtil.getAtPath(extent(), [0, LABEL, 0]) ?: DocumentUtil.getAtPath(extent(), [0, LABEL], '')
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

    boolean hasGenericTitle() {
        Util.hasGenericTitle(instanceTitle()) || Util.hasGenericTitle(workTitle())
    }

    boolean isMonograph() {
        instanceData?.issuanceType == 'Monograph'
    }

    boolean isManuscript() {
        instanceType() == 'Manuscript' || [[(ID_KEY): 'https://id.kb.se/term/saogf/Manuskript'], [(ID_KEY): 'https://id.kb.se/term/saogf/Handskrifter']].intersect(genreForm())
    }

    boolean isInSb17Bibliography() {
        asList(record()['bibliography']).contains([(ID_KEY): 'https://libris.kb.se/library/SB17'])
    }

    boolean isMaybeAggregate() {
        hasParts()
                // (s) means "samlingsverk"
                || classification().any { it[IN_SCHEME]?[CODE] =~ /[Kk]ssb/ && it[CODE]?.contains('(s)') }
                || !contribution().any { it[TYPE_KEY] == PRIMARY && it[AGENT] }
                || hasRelationshipWithContribution()
    }

    boolean intendedForMarcPreAdolescent() {
        intendedAudience().contains([(ID_KEY): 'https://id.kb.se/marc/PreAdolescent'])
    }

    boolean hasParts() {
        workData[HAS_PART] || instanceData[HAS_TITLE].findAll { it[TYPE_KEY] == TITLE }.any {
            it[HAS_PART]?.size() > 1
                    || it[HAS_PART]?.any { p -> asList(p[PART_NAME]).size() > 1
                    || asList(p[PART_NUMBER]).size() > 1 }
                    // space+semicolon indicates an aggregate if it is not preceded by a slash
                    // aggregate: Måsen ; Onkel Vanja ; Körsbärsträdgården
                    // not aggregate: En visa för de döda / Patrick Dunne ; översättning: Hans Lindeberg
                    || [it[MAIN_TITLE], it[TITLE_REMAINDER], it[SUBTITLE]].findAll().toString() =~ /(?<!\/.+)(\s+;)/
        }
    }

    boolean hasRelationshipWithContribution() {
        asList(workData['relationship']).any { r ->
            asList(r['entity']).any { e ->
                e.containsKey(CONTRIBUTION)
            }
        }
    }

    boolean isFiction() {
        isMarcFiction() || isSaogfFiction() || isSabFiction()
    }

    boolean isMarcFiction() {
        genreForm().any { it[ID_KEY] in MARC_FICTION }
    }

    boolean isMarcNotFiction() {
        genreForm().any { it[ID_KEY] in MARC_NOT_FICTION }
    }

    boolean isSaogfFiction() {
        genreForm().any { it[ID_KEY] == SAOGF_SKÖN || whelk.relations.isImpliedBy(SAOGF_SKÖN, it[ID_KEY] ?: '') }
    }

    boolean isSabFiction() {
        classification().any { it[IN_SCHEME]?[CODE] =~ /[Kk]ssb/ && it[CODE] =~ /^(H|h|uH|ufH|ugH)/ }
    }

    boolean isNotFiction() {
        // A lot of fiction has marc/NotFictionNotFurtherSpecified but then classification is usually empty
        isMarcNotFiction() && (!classification().isEmpty() && !isSabFiction())
    }

    boolean isText() {
        workData[TYPE_KEY] == 'Text'
    }

    boolean isAnonymousTranslation() {
        translationOf() && !hasAnyRole([Relator.TRANSLATOR, Relator.EDITOR, Relator.ADAPTER])
    }

    boolean hasAnyRole(List<Relator> relators) {
        contribution().any {
            asList(it[ROLE]).intersect(relators.collect { [(ID_KEY): it.iri] })
        }
    }

    boolean isDrama() {
        isSabDrama() || isGfDrama()
    }

    boolean isSabDrama() {
        classification().any { it[CODE]?.contains('Hc.02') || it[CODE]?.contains('Hce.02') }
    }

    boolean isGfDrama() {
        asList(genreForm()).any { it[ID_KEY] in DRAMA_GF }
    }

    boolean isNotRegularText() {
        Set barnGfs = [
                'https://id.kb.se/term/barngf/Mekaniska%20b%C3%B6cker',
                'https://id.kb.se/term/barngf/Pop-up-b%C3%B6cker',
                'https://id.kb.se/term/barngf/TAKK',
                'https://id.kb.se/term/barngf/Taktila%20b%C3%B6cker',
                'https://id.kb.se/term/barngf/Taktila%20illustrationer',
                'https://id.kb.se/term/barngf/Teckenspr%C3%A5k',
                'https://id.kb.se/term/barngf/Tecken%20som%20st%C3%B6d%20till%20talet',
                'https://id.kb.se/term/barngf/Bliss%20%28symbolspr%C3%A5k%29'
        ] as Set

        def saoGfTactile = 'https://id.kb.se/term/saogf/Taktila%20verk'

        asList(workData[CONTENT_TYPE]).contains([(ID_KEY): 'https://id.kb.se/term/rda/TactileText'])
                || asList(instanceData?.carrierType).any { it[ID_KEY] in ['https://id.kb.se/marc/Braille', 'https://id.kb.se/marc/TacMaterialType-b'] }
                || genreForm().any {it[ID_KEY] in barnGfs || it[ID_KEY] == saoGfTactile ||  whelk.relations.isImpliedBy(saoGfTactile, it[ID_KEY]) }
    }

    boolean isThesis() {
        genreForm().any { it == [(ID_KEY): 'https://id.kb.se/marc/Thesis'] }
    }

    boolean hasDistinguishingEdition() {
        (instanceData?[EDITION_STATEMENT] ?: '').toString().toLowerCase().contains("förk")
    }

    void addComparisonProps() {
        if (hasDistinguishingEdition()) {
            workData['_editionStatement'] = instanceData[EDITION_STATEMENT]
        }
        workData['_numPages'] = numPages()
    }

    void removeComparisonProps() {
        workData.remove('_editionStatement')
        workData.remove('_numPages')
    }

    void sortContribution() {
        // PrimaryContribution first
        contribution()?.sort { it[TYPE_KEY] != PRIMARY }
    }
}