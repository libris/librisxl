import java.util.concurrent.atomic.AtomicInteger

String where = """
    collection = 'bib'
    AND data#>'{@graph,1,identifiedBy}' @> '[{\"@type\":\"ISBN\"}]'
"""

AtomicInteger countBefore2021 = new AtomicInteger(0)
AtomicInteger count2020 = new AtomicInteger(0)

// Store "used" ISBN so that each is counted only once
Set countedIsbnValues = Collections.synchronizedSet([] as Set)

selectBySqlWhere(where) { data ->
    def (record, instance) = data.graph

    if (record.created =~ /^202[1-9]/)
        return

    List isbnValues = instance.identifiedBy.findResults { it."@type" == "ISBN" ? it.value : null }

    if (isbnValues.any {
        if (it ==~ /91[0-9]{7}[0-9X]/ && !(it in countedIsbnValues)) {
            countedIsbnValues << it
            return true
        }
        if (it ==~ /97891[0-9]{7}[0-9X]/) {
            String shortForm = it.replaceFirst("978", "")
            if (!(shortForm in countedIsbnValues)) {
                countedIsbnValues << shortForm
                return true
            }
        }
        return false
    }) {
        countBefore2021.incrementAndGet()
        if (record.created =~ /^2020/)
            count2020.incrementAndGet()
    }
}

println("Antal intanser med kb-registrerat ISBN före 2021: " + countBefore2021)
println("Antal intanser med kb-registrerat ISBN 2020: " + count2020)


