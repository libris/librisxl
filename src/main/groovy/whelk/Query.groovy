package whelk

import groovy.util.logging.Log4j2 as Log
import whelk.exception.WhelkRuntimeException

@Log
class Query {

    def query
    def fields
    def sorting
    def highlights
    def ranges
    def filters
    def facets
    def queryFacets
    def boost
    int start = 0
    int n = 50

    public static final int TERM_FACET = 0
    public static final int QUERY_FACET = 1

    Query() {}

    Query(String qstr) {
        this.query = qstr
    }

    Query(Map qmap) {
        def q = null
        if (qmap.get("query")) {
            q = qmap.get("query").first()
            log.trace("Set q: $q")
        }
        else if (qmap.get("q")) {
            q = qmap.get("q").first()
            log.trace("Set q: $q")
        }
        this.query = q
        if (qmap.get("hl")) {
            for (def hl : qmap.get("hl")) {
                log.trace("Set hl: $hl")
                addHighlight(hl)
            }
        }
        if (qmap.get("order")) {
            for (def o : qmap.get("order")) {
                def direction = "ASC"
                if (o && o.startsWith("-")) {
                    o = o.substring(1)
                    direction = "DESC"
                }
                log.trace("Set order $o ($direction)")
                addSort(o, direction)
            }
        }
        if (qmap.get("sort")) {
            for (def o : qmap.get("sort")) {
                def direction = "ASC"
                if (o && o.startsWith("-")) {
                    o = o.substring(1)
                    direction = "DESC"
                }
                log.trace("Set order $o ($direction)")
                addSort(o, direction)
            }
        }
        if (qmap.get("fields")) {
            for (def f : qmap.get("fields")) {
                log.trace("Set field: $f")
                if (f.contains(":")) {
                    addField(f.split(":")[0], new Float(f.split(":")[1]))
                } else {
                    addField(f)
                }
            }
        }
        if (qmap.get("boost")) {
            for (b in qmap.get("boost")) {
                log.trace("Set boost: $b")
                try {
                    addBoost(b.split(":")[0], new Float(b.split(":")[1]))
                } catch (Exception e) {
                    log.error("Bad user: " + e.getMessage())
                }
            }
        }
        if (qmap.get("range")) {
            for (r in qmap.get("range")) {
                log.trace("Set range: $r")
                try {
                    def v = r.split(":",2)[1].split("-")
                    addRangeFilter(r.split(":",2)[0], v[0], v[1])
                } catch (Exception e) {
                    log.error("Bad user: " + e.getMessage(), e)
                }
            }
        }
        if (qmap.get("filter")) {
            for (f in qmap.get("filter")) {
                log.trace("Set filter: $f")
                try {
                    addFilter(f.split(":", 2)[0], f.split(":", 2)[1])
                } catch (Exception e) {
                    log.error("Bad user: " + e.getMessage())
                }
            }
        }
        if (qmap.get("filters")) {
            for (f in qmap.get("filters").first().split(",")) {
                log.trace("Set filter: $f")
                try {
                    addFilter(f.split(":", 2)[0], f.split(":", 2)[1])
                } catch (Exception e) {
                    log.error("Bad user: " + e.getMessage())
                }
            }
        }
        if (qmap.get("facets")) {
            for (def fct : qmap.get("facets")) {
                log.trace("Set facet: $fct")
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
            start = new Integer(qmap.get("start").first())
            log.trace("Set start: $start")
        }
        if (qmap.get("n")) {
            n = new Integer(qmap.get("n").first())
            log.trace("Set n: $n")
        }
    }

    Query addHighlight(field) {
        if (!highlights) {
            highlights = []
        }
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

    Query addField(String field) {
        if (!fields) {
            fields = []
        }
        fields << field
        return this
    }

    Query addBoost(String field, Float boostvalue) {
        if (!this.boost) {
            this.boost = [:]
        }
        this.boost[field] = boostvalue
        return this
    }

    Query addRangeFilter(field, min, max) {
        if (!ranges) {
            ranges = [:]
        }
        ranges[field] = [min, max]
        return this
    }

    Query addFilter(field, value) {
        if (!filters) {
            filters = []
        }
        filters << [(field): value]
        return this
    }

    Query addFacet(String field) {
        if (!facets) {
            facets = []
        }
        facets << new TermFacet(field, field)
        return this
    }

    Query addFacet(String name, String field, String facetgroup=null) {
        if (!facets) {
            facets = []
        }
        facets << (facetgroup!=null ? new QueryFacet(facetgroup, name, field) : new TermFacet(name, field))
        return this
    }

    Query addScriptFieldFacet(String name, String field=null) {
        if (field) {
            facets << new ScriptFieldFacet(name, field)
        } else if (name) {
            facets << new ScriptFieldFacet(name, name)
        }
        return this
    }
}

class TermFacet {
    String name, field
    TermFacet(n, f) { this.name = n; this.field = f; }
}
class ScriptFieldFacet {
    String name, field
    ScriptFieldFacet(n, f) { this.name = n; this.field = f; }
}

class QueryFacet {
    String group, name, query
    QueryFacet(g, n, q) { this.group = g; this.name = n; this.query = q; }
}
