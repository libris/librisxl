package whelk.converter

import whelk.JsonLd
import whelk.Whelk
import whelk.component.ElasticSearch

import groovy.text.*
import groovy.text.markup.*

class JsonLD2HtmlConverter implements FormatConverter {
    Whelk whelk = null

    JsonLD2HtmlConverter(Whelk whelk = null) {
        this.whelk = whelk
    }

    Map convert(Map source, String id) {
        Map recordChipMap = whelk.jsonld.toChipAsMapByLang(source['@graph'][0],
                ElasticSearch.LANGUAGES_TO_INDEX, ElasticSearch.REMOVABLE_BASE_URIS)
        Map thingChipMap = whelk.jsonld.toChipAsMapByLang(source['@graph'][1],
                ElasticSearch.LANGUAGES_TO_INDEX, ElasticSearch.REMOVABLE_BASE_URIS)

        String recordTitle = recordChipMap['sv'].toString().trim().split(',').first()
        String thingTitle = thingChipMap['sv'].toString().trim()

        TemplateConfiguration config = new TemplateConfiguration()
        config.setAutoEscape(true)
        MarkupTemplateEngine engine = new MarkupTemplateEngine(config)
        Template template = engine.createTemplateByPath('html_resource.tpl')

        Map<String, Object> model = [
                'title': "${recordTitle} (${thingTitle})",
                'record': source['@graph'][0],
                'thing': source['@graph'][1]
        ]

        Writable output = template.make(model)
        Writer writer = new StringWriter()
        output.writeTo(writer)
        String result = writer.toString()

        return [(JsonLd.NON_JSON_CONTENT_KEY): result]
    }

    String getRequiredContentType() {
        return "application/ld+json"
    }

    String getResultContentType() {
        return "text/html"
    }
}
