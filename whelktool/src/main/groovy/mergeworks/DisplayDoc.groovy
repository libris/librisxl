package mergeworks

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

    String instanceDisplayTitle() {
        displayTitle(['hasTitle': Util.flatTitles(doc.instanceTitle())])
    }

    // TODO...
    String getDisplayText(String field) {
        if (field == 'contribution') {
            return contributorStrings().join("<br>")
        } else if (field == 'classification') {
            return classificationStrings().join("<br>")
        } else if (field == 'instance title') {
            return doc.instanceTitle() ?: ''
        } else if (field == 'instance type') {
            return doc.instanceType() ?: ''
        } else if (field == 'editionStatement') {
            return doc.editionStatement() ?: ''
        } else if (field == 'responsibilityStatement') {
            return doc.responsibilityStatement() ?: ''
        } else if (field == 'encodingLevel') {
            return doc.encodingLevel()
        } else if (field == 'publication') {
            return chipString(doc.publication())
        } else if (field == 'identifiedBy') {
            return chipString(doc.identifiedBy())
        } else if (field == 'extent') {
            return chipString(doc.extent() ?: [])
        } else if (field == 'reproductionOf') {
            return reproductionOfLink()
        } else {
            return chipString(doc.workData.getOrDefault(field, []))
        }
    }

    protected String chipString(def thing) {
        Util.chipString(thing, doc.whelk)
    }

    private String reproductionOfLink() {
        def base = Document.getBASE_URI().toString()
        def shortId = doc.reproductionOf()
                ? doc.reproductionOf()[0]['@id'].substring(base.length()).replace('#it', '')
                : ''
        return "<a href=\"#$shortId\">$shortId</a>"
    }

    String tooltip(String string, String tooltip) {
        """<abbr title="${tooltip}">${string}</abbr>"""
    }

    String link() {
        String base = Document.getBASE_URI().toString()
        String kat = "katalogisering/"
        String id = doc.document.shortId
        return base + kat + id
    }

    private List contributorStrings() {
        List path = doc.instanceData ? ['instanceOf', 'contribution'] : ['contribution']
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
        List path = doc.instanceData ? ['instanceOf', 'classification'] : ['classification']
        List classification = Util.getPathSafe(getFramed(), path, [])

        classification.collect { c ->
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
            if (doc.docItem.existsInStorage) {
                framed = JsonLd.frame(doc.thingIri(), doc.whelk.loadEmbellished(doc.shortId()).data)
            } else {
                Document copy = doc.document.clone()
                doc.whelk.embellish(copy)
                framed = JsonLd.frame(doc.thingIri(), copy.data)
            }
        }

        return framed
    }
}
