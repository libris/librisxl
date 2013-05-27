package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDCleanupFormatConverter extends BasicFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()

    def facit = [
        "about.instanceOf.title" : [":", "/"],
        "about.instanceOf.titleRemainder" : ["/"],
        "about.isbn" : [":", ";"],
        "about.identifier+.comment" : [":", ";"],
        "about.identifier+.termsOfAvailability" : [":", ";"],
        "about.instanceOf.dateOfPublication" : [",", ";"],
        "about.placeOfPublication.name" : [",", ":", ";"],
        "about.publisher+.name" : [",", ":"],
        "about.placeOfManufacture.name" : [":", ";"],
        "about.extent" : ["+", ":"],
        "about.physicalDetails" : [";", "+"],
        "about.dimensions" : [";", "+"],
        "about.edition" : ["/", "=", ","],
        "about.series+.title" : [".", ",", "=", ";"],
        "about.series+.part" : [".", ",", "=", ";"],
        "about.series+.issn" : [".", ",", "=", ";"],
    ]

    Document doConvert(Document doc) {
        def json = mapper.readValue(doc.dataAsString, Map)

        def title = json.about?.instanceOf?.get("title", null)
        def titleRemainder = json.about?.instanceOf?.get("titleRemainder", null)
        def statementOfResponsibility = json.about?.instanceOf?.get("statementOfResponsibility", null)
        def dateOfPublication = json.about?.get("dateOfPublication", null)
        def publisher = json.about?.get("publisher", null)
        def placeOfPublication = json.about?.get("placeOfPublication", null)
        def placeOfManufacture = json.about?.get("placeOfManufacture", null)
        def extent = json.about?.get("extent", null)
        def physicalDetails = json.about?.get("physicalDetails", null)
        def dimensions = json.about?.get("dimensions", null)
        def isbn = json.about?.get("isbn", null)
        def edition = json.about?.get("edition", null)
        def identifier = json.about?.get("identifier", null)
        def series = json.about?.get("series", null)

        if (isbn && isbn.size() > 1 && (isbn[-1].equals(":") || isbn[-1].equals(";"))) {
            json["about"]["isbn"] = isbn[0..-2].trim()
        }
        if (identifier && identifier instanceof List) {
            identifier.eachWithIndex() { id, index ->
                def idComment = id.get("comment", null)
                if (idComment && idComment.size() > 1 && (idComment[-1].equals(":") || idComment[-1].equals(";"))) {
                    json["about"]["identifier"][index]["comment"] = idComment[0..-2].trim()
                }
                def idTerms = id.get("termsOfAvailability", null)
                if (idTerms && idTerms.size() > 1 && (idTerms[-1].equals(":") || idTerms[-1].equals(";"))) {
                    json["about"]["identifier"][index]["termsOfAvailability"] = idTerms[0..-2].trim()
                }
            }
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
        if (dateOfPublication && dateOfPublication instanceof List) {
            dateOfPublication.eachWithIndex() { it, ind ->
                if (it && it.size() > 1 && (it[-1].equals(";") || it[-1].equals(","))) {
                    json["about"]["dateOfPublication"][ind] = it[0..-2].trim()
                }
            }
        } else {
            if (dateOfPublication && dateOfPublication.size() > 1 && (dateOfPublication[-1].equals(";") || dateOfPublication[-1].equals(","))) {
                json["about"]["dateOfPublication"] = dateOfPublication[0..-2].trim()
            }
        }
        if (placeOfPublication) {
            def place_name
            if (placeOfPublication instanceof List) {
                placeOfPublication.eachWithIndex() { pl, i ->
                    place_name = pl.get("name", null)
                    if (place_name && place_name.size() > 1) {
                        if ((publisher && place_name[-1].equals(":")) || place_name[-1].equals(",") || place_name[-1].equals(";")) {
                            json["about"]["placeOfPublication"][i]["name"] = place_name[0..-2].trim()
                        }
                    }
                }
            } else {
                place_name = placeOfPublication.get("name", null)
                if (place_name && place_name.size() > 1) {
                    if ((publisher && place_name[-1].equals(":")) || place_name[-1].equals(",") || place_name[-1].equals(";")) {
                        json["about"]["placeOfPublication"]["name"] = place_name[0..-2].trim()
                    }
                }
            }

        }
        if (placeOfManufacture) {
            def manu_name
            if (placeOfManufacture instanceof List) {
                placeOfManufacture.eachWithIndex() { it, i ->
                    manu_name = it.get("name", null)
                    if (manu_name) {
                        if (manu_name[-1].equals(";") || manu_name[-1].equals(":")) {
                            json["about"]["placeOfManufacture"]["name"] = manu_name[0..-2].trim().replaceAll("[()]","")
                        } else {
                            json["about"]["placeOfManufacture"][i]["name"] = manu_name.replaceAll("[()]","")
                        }
                    }
                }
            }
            manu_name = placeOfManufacture.get("name", null)
            if (manu_name) {
                if (manu_name[-1].equals(";") || manu_name[-1].equals(":")) {
                    json["about"]["placeOfManufacture"]["name"] = manu_name[0..-2].trim().replaceAll("[()]","")
                 } else {
                     json["about"]["placeOfManufacture"]["name"] = manu_name.replaceAll("[()]","")
                 }
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
        if (series && series instanceof List) {
            series.eachWithIndex() { x, y ->
                x.each { k, v ->
                    if (v && v.size() > 1 && (v[-1].equals(",") || v[-1].equals(".") || v[-1].equals(":") || v[-1].equals(";"))) {
                        json["about"]["series"][y][(k)] = v[0..-2].trim()
                    }
                }
            }
        }
        doc = doc.withData(mapper.writeValueAsBytes(json))
        return doc
    }
}
