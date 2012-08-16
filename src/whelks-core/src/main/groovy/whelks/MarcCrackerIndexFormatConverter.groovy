package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*

import groovy.json.*

class MarcCrackerIndexFormatConverter implements IndexFormatConverter {

    String id = this.class.name
    boolean enabled = true

    @Override
    Document convert(Document doc) {
        def json = new JsonSlurper().parseText(doc.dataAsString)
        def leader = json.leader
        def f001,f006,f007,f008
        json.fields.each { 
            it.each { key, value ->
                if (key == "001") {
                    f001 = value
                } else if (key == "006") {
                    f006 = value
                } else if (key == "007") {
                    f007 = value
                } else if (key == "008") {
                    f008 = value
                }
            }
        }
        def d = [:]
        d['00_date_entered'] = f008[0..5]
        d['06_type_of_date'] = f008[6]
        d['07_date_1'] = f008[7..10]
        d['11_date_2'] = f008[11..14]
        d['15_place'] = f008[15..17]
        d['35_language'] = f008[35..37]
        d['38_modified_record'] = f008[38]
        d['39_cataloging_source'] = f008[39]

        if (leader[6] == 'a' && leader[7] == 'm') {
            d['18_illustrations'] = f008[18..21]
            d['22_target_audience'] = f008[22]
            d['23_form_of_item'] = f008[23]
            d['24_nature_of_contents'] = f008[24..27]
            d['28_government_publication'] = f008[28]
            d['29_conference_publication'] = f008[29]
            d['30_festschrift'] = f008[30]
            d['31_index'] = f008[31]
            d['32_undefined'] = f008[32]
            d['33_literary_form'] = f008[33]
            d['34_biography'] = f008[34]
        }

        json.fields << ["008_exploded": ["subfields": d.collect {key, value -> [(key):value]}]]
        
        def builder = new JsonBuilder(json)
        doc.withData(builder.toString())

        return doc
    }


    void enable() { this.enabled = true }
    void disable() { this.enabled = false }
}
