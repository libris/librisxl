package se.kb.libris.whelks

import se.kb.libris.whelks.exception.WhelkRuntimeException

class Query {

    def query
    def fields
    def sorting
    def highlights
    def filters
    def facets
    int start = 0
    int n = 50

    Query(String qstr) {
        this.query = qstr
    }

    Query(Map qmap) {
        def q = null
        if (qmap.get("query")) {
            q = qmap.get("query")
        }
        else if (qmap.get("q")) {
            q = qmap.get("q")
        }
        if (q) {
            this.query = q
            if (qmap.get("hl")) {
                for (def hl : qmap.get("hl").split(",")) {
                    addHighlight(hl)
                }
            }
            if (qmap.get("order")) {
                for (def o : qmap.get("order").split(",")) {
                    def direction = "ASC"
                    if (o && o.startsWith("-")) {
                        o = o.substring(1)
                        direction = "DESC"
                    }
                    addSort(o, direction)
                }
            }
            if (qmap.get("fields")) {
                for (def f : qmap.get("fields").split(",")) {
                    addField(f)
                }
            }
            if (qmap.get("facets")) {
                for (def fct : qmap.get("facets").split(",")) {
                    def f = fct.split(":")
                    def flabel = null
                    def fvalue = null
                    if (f.size() > 1) {
                        flabel = f[0]
                        fvalue = f[1]
                    } else {
                        flabel = f[0]
                        fvalue = f[0]
                    }
                    addFacet(flabel, fvalue)
                }
            }
            if (qmap.get("start")) {
                start = new Integer(qmap.get("start"))
            }
            if (qmap.get("n")) {
                start = new Integer(qmap.get("n"))
            }
        } else {
            throw new WhelkRuntimeException("Trying to create empty query.")
        }
    }

    Query addHighlight(field) {
        highlights << field
        return this
    }

    Query addSort(field) {
        addSort(field, "ASC")
        return this
    }

    Query addSort(field, direction) {
        if (!sorting) {
            this.sorting = new LinkedHashMap<String, String>()
        }
        this.sorting.put(field, direction)
        return this
    }

    Query addField(field) {
        fields << field
        return this
    }

    Query addFilter(field, value) {
        if (!filters) {
            filters = {}
        }
        filters[field] = value
        return this
    }

    Query addFacet(name, field) {
        if (!facets) {
            facets = new HashMap<String,String>()
        }
        facets.put(name, field)
        return this
    }
}
