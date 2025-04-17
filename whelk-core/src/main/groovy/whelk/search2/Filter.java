package whelk.search2;

import whelk.exception.InvalidQueryException;
import whelk.search2.querytree.ActiveFilter;
import whelk.search2.querytree.InactiveFilter;
import whelk.search2.querytree.Node;
import whelk.search2.querytree.QueryTreeBuilder;

import java.util.Map;

import static whelk.JsonLd.Rdfs.RESOURCE;
import static whelk.JsonLd.TYPE_KEY;

public class Filter {
    private final String raw;
    private Node parsed;

    public Filter(String raw) {
        this.raw = raw;
    }

    public Node getParsed() {
        if (parsed == null) {
            throw new IllegalStateException("Filter has not been parsed");
        }
        return parsed;
    }

    public void parse(Disambiguate disambiguate) throws InvalidQueryException {
        if (parsed == null) {
            this.parsed = QueryTreeBuilder.buildTree(raw, disambiguate);
        }
    }

    public Filter parseAndGet(Disambiguate disambiguate) throws InvalidQueryException {
        parse(disambiguate);
        return this;
    }

    public static class AliasedFilter extends Filter {
        private final String alias;
        private final Map<String, Object> prefLabelByLang;

        public AliasedFilter(String alias, String raw, Map<String, Object> prefLabelByLang) {
            super(raw);
            this.alias = alias;
            this.prefLabelByLang = prefLabelByLang;
        }

        public String alias() {
            return alias;
        }

        public Map<String, Object> description() {
            return Map.of(TYPE_KEY, RESOURCE,
                    "prefLabelByLang", prefLabelByLang);
        }

        @Override
        public AliasedFilter parseAndGet(Disambiguate disambiguate) throws InvalidQueryException {
            return (AliasedFilter) super.parseAndGet(disambiguate);
        }

        public ActiveFilter getActive() {
            return new ActiveFilter(this);
        }

        public InactiveFilter getInactive() {
            return new InactiveFilter(this);
        }
    }
}
