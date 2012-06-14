package se.kb.libris.whelks

class Query {

    def query 
    def fields 
    def sorting
    def highlights

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
