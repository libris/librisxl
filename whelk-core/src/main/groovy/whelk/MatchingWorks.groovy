package whelk;

import whelk.search.ESQuery;
import whelk.search.ElasticFind;

public class MatchingWorks {
    Whelk m_whelk
    ElasticFind elasticFind

    private MatchingWorks() {}

    public MatchingWorks(Whelk whelk) {
        elasticFind = new ElasticFind(new ESQuery(whelk))
        m_whelk = whelk
    }

    /**
     * Find instance-records matching the passed instance-record
     *
     * These should be candidates for creating/sharing a common work
     * Returns a set of ids.
     */
    public Iterable<String> getPossibleInstanceMatches(Document instance)
    {
        Map<String, List<String>> parameters = buildInstanceQuery(instance)
        if (parameters == null)
            return []
        return elasticFind.findIds(parameters)
    }

    /**
     * Find instance-records or work-records matching the work in the passed
     * instance-record
     * Matched works are supersets (or equals) of the work passed as a
     * parameter.
     *
     * The intended purpose is to find works this instance can link to with
     * instanceOf.
     * Returns a set of ids.
     */
    public Iterable<String> getDefinitiveInstanceOfMatches(Document instance)
    {
    }

    /**
     * Find work-records
     *
     * The intended purpose is to find works this instance can link to with
     * expressionOf.
     * Returns a set of ids.
     */
    public Iterable<String> getDefinitiveExpressionOfMatches(Document instance)
    {
    }

    private Map<String, List<String>> buildInstanceQuery(instance) {
        def title = title(instance)

        if (!title)
            return null

        Map<String, List<String>> query = [
                "q"                 : ["*"],
                "@type"             : ["*"],
                "hasTitle.mainTitle": [title + "~"],
        ]

        def author = primaryContributorId(instance)
        if (author) {
            query["instanceOf.contribution.agent.@id"] = [author]
            return query
        }

        def contributors = contributorStrings(instance)
        if (contributors) {
            query["instanceOf.contribution._str"] = contributors.collect { it + "~" }
            return query
        }

        return null
    }

    private String title(instance)
    {
        return getPathSafe(instance.data, ['@graph', 1, 'hasTitle', 0, 'mainTitle'])
    }

    private String primaryContributorId(instance)
    {
        def primary = getPathSafe(instance.data, ['@graph', 2, 'contribution'], []).grep{
            it['@type'] == "PrimaryContribution"
        }
        return getPathSafe(primary, [0, 'agent', '@id'])
    }

    private List contributorStrings(instance)
    {
        return getPathSafe(m_whelk.jsonld.toCard(instance.data, false, true),
                ['@graph', 2, 'contribution'], [])['_str'].grep { it }
    }

    /*private String flatTitle(instance) {
        return flatten(
                instance.data['@graph'][1]['hasTitle'],
                ['mainTitle', 'titleRemainder', 'subtitle', 'hasPart', 'partNumber', 'partName',]
        )
    }

    private String flatten(Object o, List order) {
        if (o instanceof String) {
            return o
        }
        if (o instanceof List) {
            return o
                    .collect { flatten(it, order) }
                    .join(' || ')
        }
        if (o instanceof Map) {
            return order
                    .collect { o.get(it, null) }
                    .grep { it != null }
                    .collect { flatten(it, order) }
                    .join(' | ')
        }

        throw new RuntimeException(String.format("unexpected type: %s for %s", o.class.getName(), o))
    }*/

    private Object getPathSafe(item, path, defaultTo = null)
    {
        for (p in path)
        {
            if (item[p] != null)
            {
                item = item[p]
            } else
            {
                return defaultTo
            }
        }
        return item
    }


}