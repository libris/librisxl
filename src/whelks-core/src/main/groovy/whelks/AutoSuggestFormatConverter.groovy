package se.kb.libris.whelks.plugin.external

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.plugin.*

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

@Log
class AutoSuggestFormatConverter extends BasicPlugin implements FormatConverter, WhelkAware {

    Whelk whelk
    Whelk bibwhelk
    def w_name
    def suggest_source

    int order

    String id = "autoSuggestFormatConverter"
    ObjectMapper mapper

    AutoSuggestFormatConverter(Whelk bw) {
        this.bibwhelk = bw
        this.mapper = new ObjectMapper()
    }

    @Override
    public List<Document> convert(Document document) {
        return convert([document])
    }


    @Override
    public List<Document> convert(List<Document> documents) {
        def docs
        for  (document in documents) {
            def data = document.getDataAsString()
            def ctype = document.getContentType()

            if (ctype == "application/json") {
                def in_json = mapper.readValue(data, Map)
                def rtype = record_type(in_json["leader"])
                //println "rtype: $rtype"
                suggest_source = (rtype == "bib" ? "name" : rtype)
                w_name = (whelk ? whelk.prefix : "test");
                def sug_jsons = transform(in_json, rtype)
                for (sug_json in sug_jsons) {
                    def identifier = sug_json["identifier"]
                    def link = sug_json["link"]
                    def r = mapper.writeValueAsString(sug_json)
                    if (!docs && r) {
                        docs = []
                    }
                    if (r) {
                        docs << whelk.createDocument().withIdentifier(identifier).withData(r).withContentType("application/json").withLink(link, "basedOn")
                    } else {
                        log.warn "Conversion got no content body for $identifier."
                    }
                    /*
                    if (whelk) {
                    def mydoc = whelk.createDocument().withIdentifier(identifier).withData(r).withContentType("application/json");
                    def uri = whelk.store(mydoc)
                    }
                    */
                }
            }
        }
        log.debug("Convert resulted in " + docs?.size() + " documents.")
        return docs
    }

    def transform(a_json, rtype) {
        def alla_json = []
        def resten_json = [:]
        def link
        def id001

        def top_title


        for (def f in a_json["fields"]) {
            f.each { k, v ->
                if (k == "001") {
                    link = new String("/${rtype}/${v}")
                    id001 = v
                }
                if (k in ["100", "700"]) {
                    def should_add = true
                    def sug_json = [:]
                    sug_json["100"] = [:]
                    sug_json["100"]["ind1"] = v["ind1"]
                    for (def sf in v["subfields"]) {
                        sf.each {sk, sv -> 
                            if (sk == "0" && rtype == "bib") {
                                should_add = false
                            }
                            if (["a","b","c","d"].contains(sk)) {
                                sug_json["100"][sk] = sv
                            }
                        }
                    }
                    if (should_add) {
                        if (k == "100") {
                            sug_json["identifier"] = new String("/${w_name}/${suggest_source}/${id001}")
                        } else {
                            def name = "${id001}/" + sug_json["100"]["a"].replace(",","").replace(" ", "_").replace(".","").replace("[","").replace("]","")

                            //print "values", w_name, suggest_source, "name:", name
                            sug_json["identifier"] = new String("/${w_name}/${suggest_source}/${name}")
                        }
                        alla_json << sug_json
                    }
                } else if (k in get_fields(rtype)) {
                    def f_list = (resten_json[k] ? resten_json[k] : [])
                    def d = [:]
                    for (sf in v["subfields"]) {
                        sf.each { sk, sv -> 
                            d[sk] = sv
                        }
                    }
                    f_list << d
                    if (k == "245") {
                        def titleparts = [:]
                        for (sf in v["subfields"]) {
                            sf.each { sk, sv ->
                                if (sk in ["a", "b"]) {
                                    titleparts[sk] = sv.replaceAll(/^[\s\/\[\]]+|[\s\/\[\]]+$/, "")
                                }
                            }
                        }

                        //print "titleparts", titleparts
                        top_title = titleparts["a"] + " " + (titleparts["b"] ? titleparts["b"] : "")
                    }

                    resten_json[k] = f_list
                }
            }
        }

        resten_json["records"] = 1

        // get_records for auth-records
        if (rtype == "auth") {
            if (alla_json.size() > 0 && alla_json[0]["100"]) {
                //f_100 = " ".join(alla_json[0]["100"].values()[1:])
                def f_100 = alla_json[0]["100"]
                resten_json = get_records(f_100, resten_json)
            }
        } 
        else if (top_title) { 
            // single top-title for bibrecords
            resten_json["top_titles"] = ["http://libris.kb.se${link}" : top_title.trim()]
        }

        // More for resten
        if (link) {
            resten_json["link"] = link
        }
        resten_json["authorized"] = (rtype == "auth" ? true : false)

            // cleanup
            try {
                resten_json.pop("245")
            } catch (Exception e) {}

        // append resten on alla
        //print "resten", resten_json
        def ny_alla = []
        for (def my_json : alla_json) {
            ny_alla << my_json + resten_json
            //ny_alla.append(dict(my_json.items() + resten_json.items()))
        }

        println "record: $ny_alla"
        return ny_alla

    }

    def get_fields(rtype) {
        if (rtype == "auth") {
            return ["400", "500", "678", "680", "856"]
        }
        return ["245", "678"]
    }

    def get_records(f_100, sug_json) {
        if (bibwhelk) {
            def query_100 = []
            def query_700 = []
            f_100.each { k, v ->
                if (["a","b","c","d"].contains(k)) {
                    query_100 << "fields.100.subfields.${k}:\"${v}\""
                    query_700 << "fields.700.subfields.${k}:\"${v}\""
                }
            }
            def q_100 = query_100.join(" AND ")
            def q_700 = query_700.join(" AND ")
            def q_swe = "fields.008:swe"

            def q_all = "(($q_100) OR ($q_700)) AND $q_swe"

            def response = bibwhelk.query(q_all) 
            //print "Count: ", response.getNumberOfHits()
            sug_json["records"] = response.getNumberOfHits()

            def top_3 = [:]
            int rh = 0
            def jdoc
            for (document in response.hits) {
                jdoc = mapper.readValue(document.getDataAsString(), Map)
                def (f_001, title) = top_title_tuple(jdoc["fields"])
                top_3[f_001] = title
                if (rh++ > 2) {
                    break
                }
            }

            def top_missing = 5 - top_3.size()
            q_all = "($q_100) OR ($q_700)"

            response = bibwhelk.query(q_all) 
            //print "Count: ", response.getNumberOfHits()
            sug_json["records"] = response.getNumberOfHits()
            rh = 0
            for (document in response.hits) {
                jdoc = mapper.readValue(document.getDataAsString(), Map)
                def (f_001, title) = top_title_tuple(jdoc["fields"])
                top_3[f_001] = title
                if (rh++ < top_missing) {
                    break
                }
            }

            //print "top_3", top_3
            def only_top_3 = [:]
            top_3[0..2].each { k, v ->
                only_top_3[k] = v
            }
            sug_json["top_titles"] = only_top_3
        }
        return sug_json
    }

    def top_title_tuple(fields) {
        def f_001 = ""
        def f_245_a = ""
        def f_245_b = ""
        def f_245_n = ""
        for (field in fields) {
            field.each { k,v ->
                if (k == "001") {
                    f_001 = v
                }
                else if (k == "245") {
                    for (sf in v["subfields"]) {
                        sf.each { sk, sv ->
                            if (sk == "a") {
                                f_245_a = sv
                            }
                            else if (sk == "b") {
                                f_245_b = sv
                            }
                            else if (sk == "n") {
                                f_245_n = sv
                            }
                        }
                    }
                }

            }
        }
        def title = ""
        if (f_001 && f_245_a) {
            //title = f_245_a.trim("[:;/.]") + f_245_b.trim("[:;/]") + " " + f_245_n.trim("[:;/ ]")
            title = f_245_a.replaceAll(/^[\[:;\/\.\]]+|[\[:;\/\.\]]+$/, "") + f_245_b.replaceAll(/^[\[:;\/\]]+|[\[:;\/\]]+$/, "") + " " + f_245_n.replaceAll(/^[\[:;\/\s\]]+|[\[:;\/\s\]]+$/, "")
        }
        return [f_001, title.trim()]
    }


    def _get_records(f_100, sug_json) {
        try {
            url = "http://libris.kb.se/xsearch"
            values = ["query" : "forf:(%s) spr:swe" % f_100, "format" : "json"]

            data = urllib.urlencode(values)
            //print "XSEARCH URL: %s?%s" % (url, data)
            reply = urllib2.urlopen(url + "?" + data)

            response = reply.read().decode("utf-8")
            //print "got response", response, type(response)

            xresult = json.loads(response)["xsearch"]

            sug_json["records"] = xresult["records"]
            top_3 = xresult["list"][0..2]
            top_titles = {}
            for (p in top_3) {
                top_titles[p["identifier"]] = unicode(p["title"])
            }
            sug_json["top_titles"] = top_titles
        }
        catch (Exception e) {
            print "exception in get_records"
        }
        return sug_json
    }

    def record_type(leader) {
        if (leader && leader[6] == "z") {
            return "auth"
        }
        return "bib"
    }
}
