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

        def title = json.about?.instanceOf?.get("title")
        def titleRemainder = json.about?.instanceOf?.get("titleRemainder")
        def statementOfResponsibility = json.about?.instanceOf?.get("statementOfResponsibility")
        def publisher = json.about?.get("publisher")
        def placeOfPublication_name, dateOfPublication, placeOfManufacture_name
        try {
            placeOfPublication_name = json.about?.get("placeOfPublication")?.get("name")
            dateOfPublication = json.about?.get("dateOfPublication")
            placeOfManufacture_name = json.about?.get("placeOfManufacture")?.get("name")
        } catch (MissingMethodException mme) {
            log.error("Caught exception. Data is not as expected, maybe wrong type of record?")
        }
        def extent = json.about?.get("extent")
        def physicalDetails = json.about?.get("physicalDetails")
        def dimensions = json.about?.get("dimensions")
        def isbn = json.about?.get("isbn")
        def edition = json.about?.get("edition")


        if (isbn && isbn.size() > 1 && (isbn[-1].equals(":") || isbn[-1].equals(";"))) {
            json["about"]["isbn"] = isbn[0..-2].trim()
        }
        if (title && title.size() > 1) {
           if (titleRemainder && title[-1].equals(":")) {
               json["about"]["instanceOf"]["title"] = title[0..-2].trim()
           }
           if (statementOfResponsibility) {
               if (title.trim()[-1].equals("/")) {
                    json["about"]["instanceOf"]["title"] = title[0..-2].trim()
               }
               if (titleRemainder && titleRemainder.size() > 1 && titleRemainder[-1].equals("/")) {
                    json["about"]["instanceOf"]["titleRemainder"] = titleRemainder[0..-2].trim()
               }
           }
        }
        if (dateOfPublication && dateOfPublication.size() > 1 && (dateOfPublication[-1].equals(";") || dateOfPublication[-1].equals(","))) {
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
        if (extent && extent.size() > 1 && (extent[-1].equals("+") || extent[-1].equals(":"))) {
            json["about"]["extent"] = extent[0..-2].trim()
        }
        if (physicalDetails && physicalDetails.size() > 1 && (physicalDetails[-1].equals(";") || physicalDetails[-1].equals("+"))) {
            json["about"]["physicalDetails"] = physicalDetails[0..-2].trim()
        }
        if (dimensions && dimensions.size() > 1 && (dimensions[-1].equals(";") || dimensions[-1].equals("+"))) {
            json["about"]["dimensions"] = dimensions[0..-2].trim()
        }
        if (edition && edition.size() > 1) {
            if (edition[-1].equals("/") || edition[-1].equals("=") || edition[-1].equals(",")) {
                  json["about"]["edition"] = edition[0..-2].trim()
            }
        }

        doc = doc.withData(mapper.writeValueAsBytes(json))
        return doc
    }
}
