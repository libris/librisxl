package datatool.scripts.mergeworks

import whelk.Document
import whelk.JsonLd

class DisplayDoc {
    Doc doc
    Map framed
    
    DisplayDoc(Doc doc) {
        this.doc = doc    
    }

    private static String displayTitle(Map thing) {
        thing['hasTitle'].collect { it['@type'] + ": " + it['flatTitle'] }.join(', ')
    }

    String mainEntityDisplayTitle() {
        displayTitle(['hasTitle': Util.flatTitles(doc.getMainEntity()['hasTitle'])])
    }

    // TODO...
    String getDisplayText(String field) {
        if (field == 'contribution') {
            return contributorStrings().join("<br>")
        } else if (field == 'classification') {
            return classificationStrings().join("<br>")
        } else if (field == 'instance title') {
            return isInstance() ? (getMainEntity()['hasTitle'] ?: '') : ''
        } else if (field == 'work title') {
            // To load hasTitle from linked work in instanceOf we can use getFramed()
            // However we then need to handle that getFramed() loads linked instances in hasTitle.source
            // Prefer getMainEntity() for now
            return isInstance() ? (getMainEntity()['instanceOf']['hasTitle'] ?: '') : (getMainEntity()['hasTitle'] ?: '')
        } else if (field == 'instance type') {
            return isInstance() ? getMainEntity()['@type'] : ''
        } else if (field == 'editionStatement') {
            return getMainEntity()['editionStatement'] ?: ''
        } else if (field == 'responsibilityStatement') {
            return getMainEntity()['responsibilityStatement'] ?: ''
        } else if (field == 'encodingLevel') {
            return doc.encodingLevel()
        } else if (field == 'publication') {
            return chipString(getMainEntity()['publication'] ?: [])
        } else if (field == 'identifiedBy') {
            return chipString(getMainEntity()['identifiedBy'] ?: [])
        } else if (field == 'extent') {
            return chipString(getMainEntity()['extent'] ?: [])
        } else if (field == 'reproductionOf') {
            return reproductionOfLink()
        } else {
            return chipString(getWork().getOrDefault(field, []))
        }
        
        
    }

    protected String chipString(def thing) {
        Util.chipString(thing, doc.whelk)
    }

    private String reproductionOfLink() {
        def shortId = Util.getPathSafe(getMainEntity(), ['reproductionOf', '@id'])
                ?.tokenize("/#")
                ?.dropRight(1)
                ?.last() ?: ''

        return "<a href=\"#$shortId\">$shortId</a>"
    }

    String tooltip(String string, String tooltip) {
        """<abbr title="${tooltip}">${string}</abbr>"""
    }

    String link() {
        String base = Document.getBASE_URI().toString()
        String kat = "katalogisering/"
        String id = doc.doc.shortId
        return base + kat + id
    }
    
    private List contributorStrings() {
        List path = isInstance() ? ['instanceOf', 'contribution'] : ['contribution']
        List contribution = Util.getPathSafe(getFramed(), path, [])

        return contribution.collect { Map c ->
            contributionStr(c)
        }
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

    List classificationStrings() {
        List path = isInstance() ? ['instanceOf', 'classification'] : ['classification']
        List<Map> classification = Util.getPathSafe(getFramed(), path, [])
        classification.collect() { c ->
            StringBuilder s = new StringBuilder()
            s.append(flatMaybeLinked(c['inScheme'], ['code', 'version']).with { it.isEmpty() ? it : it + ': ' })
            s.append(flatMaybeLinked(c, ['code']))
            return s.toString()
        }
    }

    private static String flatMaybeLinked(Object thing, List order) {
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

    static String flatten(Object o, List order, String mapSeparator = ': ') {
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
                    .findResults { ((Map) o).get(it) }
                    .collect { flatten(it, order) }
                    .join(mapSeparator)
        }

        throw new RuntimeException(String.format("unexpected type: %s for %s", o.class.getName(), o))
    }

    Map getFramed() {
        if (!framed) {
            if (isInstance()) {
                framed = JsonLd.frame(doc.doc.getThingIdentifiers().first(), doc.whelk.loadEmbellished(doc.doc.shortId).data)
            } else {
                Document copy = doc.doc.clone()
                doc.whelk.embellish(copy)
                framed = JsonLd.frame(doc.doc.getThingIdentifiers().first(), copy.data)
            }
        }

        return framed
    }

    Map getMainEntity() {
        return doc.getMainEntity()
    }

    boolean isInstance() {
        return doc.isInstance()
    }

    Map getWork() {
        return doc.getWork()
    }
}
