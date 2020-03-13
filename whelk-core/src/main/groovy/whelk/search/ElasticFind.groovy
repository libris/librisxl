package whelk.search

import groovy.transform.CompileStatic

@CompileStatic
class ElasticFind {
    private static final int PAGE_SIZE = 50

    ESQuery esQuery

    ElasticFind(ESQuery esQuery) {
        this.esQuery = esQuery
    }

    Iterable<String> findIds(Map<String, List<String>> parameters) {
        def q = { int offset -> esQuery.doQueryIds(makeParams(parameters, offset), null) }
        return query(q)
    }

    Iterable<Map> find(Map<String, List<String>> parameters) {
        def q = { int offset -> esQuery.doQuery(makeParams(parameters, offset), null) }
        return query(q)
    }

    private <T> Iterable<T> query(Closure<Map> getter) {
        Iterator<T> i = new Iterator<T>() {
            boolean beforeFirstFetch = true

            int total = 0
            int page = 0
            int ix = 0
            List<T> items

            @Override
            boolean hasNext() {
                if (beforeFirstFetch) {
                    fetchFirst()
                }

                return page * PAGE_SIZE + ix < total
            }

            @Override
            T next() {
                if (beforeFirstFetch) {
                    fetchFirst()
                }

                if (!hasNext()) {
                    throw new NoSuchElementException()
                }

                if (ix >= items.size()) {
                    fetch()
                }

                return items[ix++]
            }

            private void fetch() {
                page++
                def offset = PAGE_SIZE * page
                items = (List<T>) getter(offset)['items']
                ix = 0
            }

            private void fetchFirst() {
                def firstResult = getter(0)
                total = firstResult['totalHits']
                items = (List<T>) firstResult['items']

                beforeFirstFetch = false
            }
        }

        return new Iterable<T>() {
            @Override
            Iterator<T> iterator() {
                return i
            }
        }
    }

    private Map<String, String[]> makeParams(Map<String, List<String>> parameters, int offset) {
        Map<String, String[]> p = new HashMap<>()
        for (String key : parameters.keySet()) {
            List<String> l = parameters.get(key)
            p.put(key, l.toArray(new String[l.size()]))
        }

        p.put("_offset", [Integer.toString(offset)] as String[])
        p.put("_limit", [Integer.toString(PAGE_SIZE)] as String[])

        p.putIfAbsent("_sort", ["_doc"] as String[])

        return p
    }
}
