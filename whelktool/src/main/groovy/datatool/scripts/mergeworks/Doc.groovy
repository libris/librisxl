package datatool.scripts.mergeworks

import se.kb.libris.Normalizers
import whelk.Document
import whelk.JsonLd
import whelk.Whelk

class Doc {
    public static final String SAOGF_SKÖN = 'https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur'
    public static final String MARC_FICTION = 'https://id.kb.se/marc/FictionNotFurtherSpecified'
    Whelk whelk
    Document doc
    Map work
    Map framed
    List<String> titles

    //FIXME
    Document ogDoc

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

    static Map getWork(Whelk whelk, Document d) {
        Map work = Normalizers.getWork(whelk.jsonld, d)
        if (!work) {
            throw new NoWorkException(d.shortId)
        }
        work = new HashMap<>(work)

        //TODO 'marc:fieldref'

        work.remove('@id')
        return work
    }

    Map getInstance() {
        return doc.data['@graph'][1]
    }

    List<String> getTitleVariants() {
        if (!titles) {
            titles = Util.getTitleVariants(getInstance()['hasTitle'])
        }

        return titles
    }
    
    boolean hasGenericTitle() {
        Util.hasGenericTitle(getInstance()['hasTitle'])
    }

    private static String displayTitle(Map thing) {
        thing['hasTitle'].collect { it['@type'] + ": " + it['flatTitle'] }.join(', ')
    }

    String instanceDisplayTitle() {
        displayTitle(['hasTitle': Util.flatTitles(getInstance()['hasTitle'])])
    }

    String link() {
        String base = Document.getBASE_URI().toString()
        String kat = "katalogisering/"
        String id = doc.shortId
        return base + kat + id
    }

    boolean isMonograph() {
        getInstance()['issuanceType'] == 'Monograph'
    }

    boolean hasPart() {
        getWork()['hasPart'] != null
    }

    String encodingLevel() {
        return doc.data['@graph'][0]['encodingLevel'] ?: ''
    }
    
    int numPages() {
        String extent = Util.getPathSafe(getInstance(), ['extent', 0, 'label', 0]) ?: Util.getPathSafe(getInstance(), ['extent', 0, 'label'], '')
        return numPages(extent)
    }
    
    static int numPages(String extentLabel) {
        def matcher = extentLabel =~ /(\d+)(?=[, \[\]0-9]*[sp])/
        List<Integer> pages = []
        while (matcher.find()) {
            pages << Integer.parseInt(matcher.group(1))
        }
        pages ? pages.max() : -1
    }

    // TODO...
    String getDisplayText(String field) {
        if (field == 'contribution') {
            return contributorStrings().join("<br>")
        }
        else if (field == 'classification') {
            return classificationStrings().join("<br>")
        }
        else if (field == 'instance title') {
            return getInstance()['hasTitle'] ?: ''
        }
        else if (field == 'work title') {
            return getFramed()['instanceOf']['hasTitle'] ?: ''
        }
        else if (field == 'instance type') {
            return getInstance()['@type']
        }
        else if (field == 'editionStatement') {
            return getInstance()['editionStatement'] ?: ''
        }
        else if (field == 'responsibilityStatement') {
            return getInstance()['responsibilityStatement'] ?: ''
        }
        else if (field == 'encodingLevel') {
            return encodingLevel()
        }
        else if (field == 'publication') {
            return chipString(getInstance()['publication'] ?: [])
        }
        else if (field == 'identifiedBy') {
            return chipString(getInstance()['identifiedBy'] ?: [])
        }
        else if (field == 'extent') {
            return chipString(getInstance()['extent'] ?: [])
        }
        else {
            return chipString(getWork().getOrDefault(field, []))
        }
    }

    private String chipString (def thing) {
        if (thing instanceof Integer) {
            return thing
        }
        
        def chips = whelk.jsonld.toChip(thing)
        if (chips.size() < 2) {
            chips = thing
        }
        if (chips instanceof List) {
            return chips.collect{ valuesString(it) }.sort().join('<br>')
        }
        return valuesString(chips)
    }

    private String valuesString (def thing) {
        if (thing instanceof List) {
           return thing.collect{ valuesString(it) }.join(' • ')
        }
        if (thing instanceof Map) {
            return thing.findAll { k, v -> k != '@type'}.values().collect{ valuesString(it) }.join(' • ')
        }
        return thing.toString()
    }

    String tooltip(String string, String tooltip) {
        """<abbr title="${tooltip}">${string}</abbr>"""
    }

    private List classificationStrings() {
        List<Map> classification =  Util.getPathSafe(getFramed(), ['instanceOf', 'classification'], [])
        classification.collect() { c ->
            StringBuilder s = new StringBuilder()
            s.append(flatMaybeLinked(c['inScheme'], ['code', 'version']).with { it.isEmpty() ? it : it + ': ' })
            s.append(flatMaybeLinked(c, ['code']))
            return s.toString()
        }
    }

    private List contributorStrings() {
        List contribution = Util.getPathSafe(getFramed(), ['instanceOf', 'contribution'], [])

        return contribution.collect { Map c ->
            contributionStr(c)
        }
    }

    protected Map getFramed() {
        if(!framed) {
            framed = JsonLd.frame(doc.getThingIdentifiers().first(), whelk.loadEmbellished(doc.shortId).data)
        }

        return framed
    }

    private String contributionStr(Map contribution) {
        StringBuilder s = new StringBuilder()

        if (contribution['@type'] == 'PrimaryContribution') {
            s.append('<b>')
        }

        s.append(flatMaybeLinked(contribution['role'], ['code', 'label']).with { it.isEmpty() ? it : it + ': ' })
        s.append(flatMaybeLinked(contribution['agent'], ['givenName', 'familyName', 'lifeSpan', 'name']))

        if (contribution['@type'] == 'PrimaryContribution') {
            s.append('</b>')
        }

        return s.toString()
    }

    private static String flatten(Object o, List order, String mapSeparator = ': ') {
        if (o instanceof String) {
            return o
        }
        if (o instanceof List) {
            return o
                    .collect { flatten(it, order) }
                    .join(' || ')
        }
        if (o instanceof Map) {
            return order
                    .collect { ((Map)o).get(it, null) }
                    .grep { it != null }
                    .collect { flatten(it, order) }
                    .join(mapSeparator)
        }

        throw new RuntimeException(String.format("unexpected type: %s for %s", o.class.getName(), o))
    }

    private String flatMaybeLinked(Object thing, List order) {
        if (!thing)
            return ''

        if (thing instanceof List) {
            return thing.collect { flatMaybeLinked(it, order) }.join(' | ')
        }
        String s = flatten(thing, order, ', ')

        thing['@id']
                ? """<a href="${thing['@id']}">$s</a>"""
                : s
    }

    boolean isFiction() {
        isMarcFiction() || isSaogfFiction() || isSabFiction()
    }

    boolean isMarcFiction() {
        (getWork()['genreForm'] ?: []).any{ it['@id'] == MARC_FICTION }
    }

    boolean isSaogfFiction() {
        (getWork()['genreForm'] ?: []).any{ whelk.relations.isImpliedBy(SAOGF_SKÖN, it['@id'] ?: '') }
    }

    boolean isSabFiction() {
        classificationStrings().any{ it.contains('kssb') && it.contains(': H') }
    }

    boolean isText() {
        getWork()['@type'] == 'Text'
    }

    boolean hasDistinguishingEdition() {
        (getInstance()['editionStatement'] ?: '').toString().toLowerCase().contains("förk")
    }

    void addComparisonProps() {
        if(hasDistinguishingEdition()) {
            addToWork('editionStatement')
        }
        getWork()['numPages'] = numPages()
    }

    void addToWork(String field) {
        getWork()[field] = getInstance()[field]
    }

    void removeComparisonProps() {
        getWork().remove('editionStatement')
        getWork().remove('numPages')
    }
}

//TODO
class Doc2 extends Doc {
    Doc2(Whelk whelk, Document doc) {
        super(whelk, doc)
    }

    @Override
    String getDisplayText(String field) {
        return getWork()[field]
    }

    protected Map getFramed() {
        if(!framed) {
            Document copy = doc.clone()
            whelk.embellish(copy)
            framed = JsonLd.frame(doc.getThingIdentifiers().first(), copy.data)
        }

        return framed
    }
}