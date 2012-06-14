package se.kb.libris.whelks

class Query {

    def query 
    def fields 
    def sorting
    def highlights
    int start = 0
    int n = 50

    Query(qstr) {
        this.query = qstr
    }

    Query addHighlight(field) {
        highlights << field
    }

    Query addSort(field) {
        addSort(field, "ASC")
    }

    Query addSort(field, direction) {
        if (!sorting) {
            this.sorting = new LinkedHashMap<String, String>()
        }
        this.sorting.put(field, direction)
    }

    Query addField(field) {
        fields << field
    }
}
