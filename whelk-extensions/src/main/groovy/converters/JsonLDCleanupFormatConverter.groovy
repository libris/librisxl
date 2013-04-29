package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDCleanupFormatConverter extends BasicFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()

    Document doConvert(Document doc) {
        def json = mapper.readValue(doc.dataAsString, Map)

        def title = json.about.instanceOf?.get("title")
        def titleRemainder = json.about.instanceOf?.get("titleRemainder")
        def statementOfResponsibility = json.about.instanceOf?.get("statementOfResponsibility")
        def cleaned_title = title
        def cleaned_titleRemainder = titleRemainder
        if (title) {
           if (titleRemainder && title[-1].equals(":")) {
               cleaned_title = title[0..-2].trim()
           }
           if (statementOfResponsibility) {
               if (title.trim()[-1].equals("/")) {
                    cleaned_title = title[0..-2].trim()
               }
               if (titleRemainder && titleRemainder[-1].equals("/")) {
                    cleaned_titleRemainder = titleRemainder[0..-2].trim()
               }
           }
           json["about"]["instanceOf"]["title"] = cleaned_title
           json["about"]["instanceOf"]["titleRemainder"] = cleaned_titleRemainder
        }
        def publisher = json.about?.get("publisher")
        def placeOfPublication_name = json.about?.get("placeOfPublication")?.get("name")
        def dateOfPublication = json.about.get("dateOfPublication")
        def placeOfManufacture_name = json.about?.get("placeOfManufacture")?.get("name")
        if (dateOfPublication) {
            if (dateOfPublication.size() > 1 && (dateOfPublication[-1].equals(";") || dateOfPublication[-1].equals(","))) {
                json["about"]["dateOfPublication"] = dateOfPublication[0..-2].trim()
            }
            if (placeOfPublication_name && placeOfPublication_name.size() > 1) {
                if ((json.about?.get("publisher") && placeOfPublication_name[-1].equals(":")) || placeOfPublication_name[-1].equals(",") || placeOfPublication_name[-1].equals(";")) {
                    json["about"]["placeOfPublication"]["name"] = placeOfPublication_name[0..-2].trim()
                }
            } 
            if (placeOfManufacture_name) {
                if (placeOfManufacture_name[-1].equals(";")) {
                    json["about"]["placeOfManufacture"]["name"] = placeOfManufacture_name[0..-2].trim().replaceAll("[()]","")
                } else {
                    json["about"]["placeOfManufacture"]["name"] = placeOfManufacture_name.replaceAll("[()]","")
                }
            }
            if (publisher && publisher instanceof List) {
                publisher.eachWithIndex() { it, i ->
                    if (it["name"] && it["name"].size() > 1 && (it["name"][-1].equals(",") || it["name"][-1].equals(":"))) {
                        json["about"]["publisher"][i]["name"] = it["name"][0..-2].trim()
                    }
                }
            }
        }
        doc = doc.withData(mapper.writeValueAsBytes(json))
        //TODO: clean up interpunction 260, 300
        return doc
    }
}