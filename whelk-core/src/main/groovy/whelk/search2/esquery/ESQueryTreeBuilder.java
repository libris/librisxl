package whelk.search2.esquery;

import whelk.search.QueryDateTime;
import whelk.search2.Operator;
import whelk.search2.QueryUtil;
import whelk.search2.querytree.node.And;
import whelk.search2.querytree.value.Any;
import whelk.search2.querytree.node.Condition;
import whelk.search2.querytree.value.DateTime;
import whelk.search2.querytree.node.FilterAlias;
import whelk.search2.querytree.value.FreeText;
import whelk.search2.querytree.value.InvalidValue;
import whelk.search2.querytree.value.Link;
import whelk.search2.querytree.node.Node;
import whelk.search2.querytree.node.Not;
import whelk.search2.querytree.node.Or;
import whelk.search2.querytree.QueryTree;
import whelk.search2.querytree.value.Term;
import whelk.search2.querytree.value.Token;
import whelk.search2.querytree.value.Value;
import whelk.search2.querytree.value.VocabTerm;
import whelk.search2.querytree.value.YearRange;
import whelk.util.Unicode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static whelk.JsonLd.ID_KEY;
import static whelk.JsonLd.SEARCH_KEY;
import static whelk.search2.esquery.ESMappings.FOUR_DIGITS_KEYWORD_SUFFIX;
import static whelk.search2.esquery.ESMappings.FOUR_DIGITS_SHORT_SUFFIX;
import static whelk.search2.esquery.ESMappings.KEYWORD;
import static whelk.search2.QueryUtil.quote;

public class ESQueryTreeBuilder {
    public static ESNode buildFrom(Node queryTreeNode, ESSettings settings) {
        return switch (queryTreeNode) {
            case Condition c -> buildFromCondition(c, settings);
            case FilterAlias fa -> buildFrom(fa.getParsed(), settings);
            case And and -> buildFromAnd(and, settings);
            case Or or -> buildFromOr(or, settings);
            case Not not -> buildFromNot(not, settings);
        };
    }

    private static ESNode buildFromNot(Not not, ESSettings esSettings) {
        return new ESNode.MustNot(buildFrom(not.node(), esSettings));
    }

    private static ESNode buildFromOr(Or or, ESSettings esSettings) {
        List<ESNode> subQueries = or.children().stream()
                .map(n -> buildFrom(n, esSettings))
                .toList();

        return or instanceof QueryTree.ExpandedTree.DerivedOr derivedOr
                ? buildDerivedDisjunction(derivedOr.originalCondition(), subQueries)
                : factorOutNested(new ESNode.Should(subQueries));
    }

    private static ESNode buildDerivedDisjunction(Condition origCondition, List<ESNode> subQueries) {
        // TODO: Explaining comments
        ESNode.DisMax disMax = new ESNode.DisMax(subQueries);
        ESNode query = factorOutNested(disMax);

        boolean shouldCombineFields = origCondition.selector().isComposite();

        if (shouldCombineFields) {
            return query instanceof ESNode.Nested nested
                    ? nested.withInnerQuery(combineFields((ESNode.DisMax) nested.query()))
                    : combineFields(disMax);
        }

        return query;
    }

    private static ESNode factorOutNested(ESNode.Disjunction dis) {
        boolean allSubQueriesAreNested = dis.subQueries().stream().allMatch(ESNode.Nested.class::isInstance);
        if (!allSubQueriesAreNested) {
            return dis;
        }

        List<ESNode.Nested> nestedSubQueries = dis.subQueries().stream()
                .map(ESNode.Nested.class::cast)
                .toList();

        boolean differentStems = nestedSubQueries.stream().map(ESNode.Nested::stem).distinct().count() > 1;
        if (differentStems) {
            return dis;
        }

        List<ESNode> subQueriesAsNonNested = nestedSubQueries.stream()
                .map(ESNode.Nested::query)
                .toList();
        ESNode.NestedStem stem = nestedSubQueries.getFirst().stem();
        Set<ESNode.NestedField> nestedFields = nestedSubQueries.stream()
                .map(ESNode.Nested::fields)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        return new ESNode.Nested(dis.withSubQueries(subQueriesAsNonNested), stem, nestedFields);
    }

    private static ESNode combineFields(ESNode.Disjunction dis) {
        List<ESNode> subQueries = combineSimpleTextQueryFields(dis.subQueries());
        return subQueries.size() == 1
                ? subQueries.getFirst()
                : dis.withSubQueries(subQueries);
    }

    private static List<ESNode> combineSimpleTextQueryFields(List<ESNode> subQueries) {
        List<ESNode.TextQuery> simpleQueries = subQueries.stream()
                .filter(subQuery -> subQuery instanceof ESNode.TextQuery t && t.isSimple())
                .map(ESNode.TextQuery.class::cast)
                .toList();

        if (simpleQueries.size() < 2) {
            return subQueries;
        }

        List<ESBoost.Field> mergedFields = simpleQueries.stream()
                .flatMap(q -> q.boostFields().stream())
                .toList();

        ESNode.TextQuery first = simpleQueries.getFirst();
        int firstIdx = subQueries.indexOf(first);

        List<ESNode> result = new ArrayList<>(subQueries);
        result.removeAll(simpleQueries);
        result.add(firstIdx, first.withFields(mergedFields));

        return result;
    }

    private static ESNode buildFromAnd(And and, ESSettings esSettings) {
        List<ESNode> subQueries = and.children().stream()
                .map(n -> buildFrom(n, esSettings))
                .toList();
        ESNode.Must must = new ESNode.Must(subQueries);
        return groupNested(must);
    }

    private static ESNode groupNested(ESNode.Must must) {
        List<ESNode> subQueries = new ArrayList<>();

        AtomicReference<ESNode.NestedStem> currentNestedStem = new AtomicReference<>();
        List<ESNode.Nested> currentGroup = new ArrayList<>();

        Runnable collectNested = () -> {
            if (currentGroup.size() == 1) {
                subQueries.add(currentGroup.getFirst());
            }
            else if (currentGroup.size() > 1){
                List<ESNode> subQueriesAsNonNested = currentGroup.stream()
                        .map(ESNode.Nested::query)
                        .toList();
                Set<ESNode.NestedField> nestedFields = currentGroup.stream()
                        .map(ESNode.Nested::fields)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
                ESNode.Nested outerNested = new ESNode.Nested(new ESNode.Must(subQueriesAsNonNested),
                        currentNestedStem.get(),
                        nestedFields);
                subQueries.add(outerNested);
            }

            currentNestedStem.set(null);
            currentGroup.clear();
        };

        Predicate<ESNode.Nested> isCompatibleWithCurrentGroup = n -> {
            ESNode.NestedStem stem = n.stem();
            Set<ESNode.NestedField> fields = n.fields();
            Set<ESNode.NestedField> currentFields = currentGroup.stream()
                    .map(ESNode.Nested::fields)
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());
            return stem.equals(currentNestedStem.get())
                    && fields.stream().noneMatch(f -> !f.isRepeatable() && currentFields.contains(f));
        };

        for (ESNode subQuery : must.subQueries()) {
            if (subQuery instanceof ESNode.Nested nested) {
                if (isCompatibleWithCurrentGroup.test(nested)) {
                    currentGroup.add(nested);
                } else {
                    // Collect previous group
                    collectNested.run();
                    // Then begin new group
                    currentNestedStem.set(nested.stem());
                    currentGroup.add(nested);
                }
            } else if (subQuery instanceof ESNode.MustNot(ESNode.Nested nested)
                    // A must_not(nested(...)) clause may only join an existing nested group if that group
                    // already contains a "positive" nested clause for the same path.
                    && isCompatibleWithCurrentGroup.test(nested)) {
                // Move the negation inside the nested query so it can be grouped with the existing nested clauses
                ESNode.MustNot nonNested = new ESNode.MustNot(nested.query());
                ESNode.Nested outerNested = new ESNode.Nested(nonNested, nested.stem(), Set.of());
                currentGroup.add(outerNested);
            } else {
                // A non-nested sub-query works as a separator for nested groups
                collectNested.run(); // Collect previous group
                subQueries.add(subQuery);
            }
        }

        collectNested.run();

        return subQueries.size() == 1
                ? subQueries.getFirst()
                : new ESNode.Must(subQueries);
    }

    private static ESNode buildFromCondition(Condition c, ESSettings esSettings) {
        if (c.isAnyQuery()) {
            return new ESNode.MatchAll();
        }
        if (c.isTextQuery()) {
            return buildFromFreeText(c.freeTextValue(), esSettings.boost());
        }

        String f = c.selector().esField();
        Operator op = c.operator();
        Value v = c.value();

        ESNode query = switch (v) {
            case Any ignored -> new ESNode.Exists(f);
            case DateTime dateTime -> buildFromDateTimeValue(f, op, dateTime, esSettings);
            case FreeText freeText -> buildFromFreeTextValue(f, op, freeText, esSettings, c.selector().isObjectProperty());
            case InvalidValue ignored -> new ESNode.MatchNone();
            case Link link -> buildFromLinkValue(f, op, link, esSettings);
            case VocabTerm vocabTerm -> new ESNode.TermQuery(f, vocabTerm.jsonForm());
            case Term term -> new ESNode.TermQuery(f, term.term());
            case YearRange yearRange -> buildFromYearRangeValue(f, op, yearRange, esSettings);
        };

        if (c.isFlaggedForPostFilter()) {
            query = new ESNode.PostFilter(query);
        }

        ESMappings esMappings = esSettings.mappings();

        Optional<String> stem = getNestedStem(f, esMappings);
        if (stem.isPresent()) {
            boolean includeInParent = esMappings.isNestedIncludeInParentField(stem.get());
            boolean isRepeatable = c.selector().isLdSetContainer();
            ESNode.NestedStem nestedStem = new ESNode.NestedStem(stem.get(), includeInParent);
            ESNode.NestedField nestedField = new ESNode.NestedField(f, isRepeatable);
            return new ESNode.Nested(query, nestedStem, Set.of(nestedField));
        }

        return query;
    }

    private static ESNode buildFromFreeText(FreeText ft, ESBoost esBoost) {
        return buildTextQuery(ft, esBoost.freeTextFields(), esBoost.freeTextQuerySettings());
    }

    private static ESNode buildFromDateTimeValue(String field, Operator operator, DateTime dateTime, ESSettings esSettings) {
        if (esSettings.mappings().isDateTypeField(field)) {
            return switch (operator) {
                case EQUALS -> new ESNode.TermQuery(field, dateTime.dateTime().toElasticDateString());
                case GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS -> new ESNode.RangeQuery(field, Map.of(operator, dateTime.dateTime().toElasticDateString()));
                case LIKE -> new ESNode.MatchNone(); // Makes no sense
            };
        }
        return buildFieldedTextQuery(field, new FreeText(dateTime.toString()), esSettings);
    }

    private static ESNode buildFromFreeTextValue(String field, Operator operator, FreeText ft, ESSettings esSettings, boolean addSearchKey) {
        return switch (operator) {
            case EQUALS, LIKE -> {
                if (isLikelyTextField(field, esSettings.mappings())) {
                    String textField = addSearchKey ? String.format("%s.%s", field, SEARCH_KEY) : field;
                    yield buildFieldedTextQuery(textField, ft, esSettings);
                } else {
                    yield buildPerTokenTermQueriesQuery(field, ft, esSettings);
                }
            }
            case LESS_THAN, GREATER_THAN, LESS_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS -> {
                if (ft.isRangeOpCompatible() && esSettings.mappings().isRangeQueryCompatible(field)) {
                    String digitsField = esSettings.mappings().hasFourDigitsShortField(field)
                            ? field + FOUR_DIGITS_SHORT_SUFFIX
                            : field;
                    long limit = Long.parseLong(ft.toEsString());
                    yield new ESNode.RangeQuery(digitsField, Map.of(operator, limit));
                } else {
                    yield new ESNode.MatchNone();
                }
            }
        };
    }

    private static ESNode buildFromLinkValue(String field, Operator operator, Link link, ESSettings esSettings) {
        return operator == Operator.LIKE && !"".equals(link.getNeedle())
                ? buildWithNeedle(field, link, esSettings.mappings())
                : new ESNode.TermQuery(String.format("%s.%s", field, ID_KEY), link.jsonForm());
    }

    private static ESNode buildWithNeedle(String field, Link link, ESMappings esMappings) {
        String needle = Arrays.stream(link.getNeedle().split("\\s"))
                .map(QueryUtil::quote)
                .collect(Collectors.joining(" "));

        String idField = String.format("%s.%s", field, ID_KEY);
        String strField = String.format("%s.%s", field, SEARCH_KEY);

        ESNode idQuery = new ESNode.TermQuery(idField, link.jsonForm());
        ESNode textQuery = ESNode.TextQuery.simpleUnboostedQuery(needle, strField);

        ESNode notLinked = isNested(field, esMappings)
                ? new ESNode.MustNot(new ESNode.Exists(idField))
                : new ESNode.MustNot(idQuery);
        ESNode.Must blankQuery = new ESNode.Must(List.of(textQuery, notLinked));

        float linkedBeforeBlank = 50_000f;
        ESNode boostedIdQuery = new ESNode.ConstantScore(idQuery, linkedBeforeBlank);

        return new ESNode.Should(List.of(boostedIdQuery, blankQuery));
    }

    private static ESNode buildPerTokenTermQueriesQuery(String field, FreeText ft, ESSettings esSettings) {
        ESMappings mappings = esSettings.mappings();

        // Known placeholder values (0000, 9999) are excluded from 4-digit fields to prevent them from being treated as valid years in sorting and aggregations.
        Predicate<String> isFourDigitsFieldValue = s -> s.length() == 4 && !s.equals("0000") && !s.equals("9999");

        List<ESNode> perTokenTermQueries = new ArrayList<>();

        for (Token t : ft.tokens()) {
            String v = t.value();
            if (mappings.hasFourDigitsKeywordField(field) && t.isDigits() && isFourDigitsFieldValue.test(v)) {
                perTokenTermQueries.add(new ESNode.TermQuery(field + FOUR_DIGITS_KEYWORD_SUFFIX, v));
            } else if (mappings.hasKeywordSubfield(field) && !isMaskedOrTruncated(v)) {
                perTokenTermQueries.add(new ESNode.TermQuery(String.format("%s.%s", field, KEYWORD), v));
            } else if (mappings.isLongTypeField(field) && t.isDigits()) {
                perTokenTermQueries.add(new ESNode.TermQuery(field, Long.parseLong(v)));
            } else {
                return buildFieldedTextQuery(field, ft, esSettings);
            }
        }

        if (perTokenTermQueries.size() == 1) {
            return perTokenTermQueries.getFirst();
        }

        return switch (ft.connective()) {
            case OR -> new ESNode.Should(perTokenTermQueries);
            case AND -> new ESNode.Must(perTokenTermQueries);
        };
    }

    private static ESNode buildFromYearRangeValue(String field, Operator operator, YearRange yearRange, ESSettings esSettings) {
        if (operator == Operator.EQUALS) {
            if (esSettings.mappings().hasFourDigitsShortField(field)) {
                return buildFromYearRangeValue(field + FOUR_DIGITS_SHORT_SUFFIX, yearRange, Integer::parseInt);
            } else if (esSettings.mappings().isDateTypeField(field)) {
                return buildFromYearRangeValue(field, yearRange, v -> QueryDateTime.parse(v).toElasticDateString());
            } else {
                return buildFieldedTextQuery(field, new FreeText(yearRange.toString()), esSettings);
            }
        }

        return new ESNode.MatchNone(); // Makes no sense
    }

    private static ESNode buildFromYearRangeValue(String field, YearRange yearRange, Function<String, Object> parseLimit) {
        Map<Operator, Object> rangeMap = new HashMap<>();

        if (!yearRange.min().isEmpty()) {
            rangeMap.put(Operator.GREATER_THAN_OR_EQUALS, parseLimit.apply(yearRange.min()));
        }
        if (!yearRange.max().isEmpty()) {
            rangeMap.put(Operator.LESS_THAN_OR_EQUALS, parseLimit.apply(yearRange.max()));
        }

        return new ESNode.RangeQuery(field, rangeMap);
    }

    private static ESNode buildFieldedTextQuery(String f, FreeText ft, ESSettings esSettings) {
        ESBoost.FieldedQuerySettings boostSettings = esSettings.boost().fieldedQuerySettings();
        ESBoost.Field field = new ESBoost.Field(f, boostSettings.defaultBoostFactor());
        return buildTextQuery(ft, List.of(field), boostSettings);
    }

    private static ESNode buildTextQuery(FreeText ft, List<ESBoost.Field> fields, ESBoost.TextQuerySettings boostSettings) {
        String s = ft.toEsString();
        s = Unicode.normalizeForSearch(s);

        // TODO search for original string OR stripped string?
        if (Unicode.looksLikeIsbn(s) && s.contains("-")) {
            s = s.replace("-", "");
        }

        ESNode.TextQueryMode textQueryMode = isSimple(s)
                ? new ESNode.SimpleQueryString(s)
                : new ESNode.QueryString(escapeNonSimpleQueryString(s), ESNode.MultiMatchType.most_fields);

        ESNode.TextQuery baseQuery = new ESNode.TextQuery(textQueryMode, fields, ft.connective(), boostSettings);

        if (boostSettings.boostPhrase()) {
            return buildWithPhraseBoost(baseQuery, ft.tokens());
        }

        return buildWithNormalizers(baseQuery);
    }

    private static ESNode buildWithNormalizers(ESNode.TextQuery query) {
        List<ESBoost.Field> fields = query.boostFields();
        List<ESBoost.ScriptScoreNormalizer> normalizers = collectNormalizers(fields);

        if (normalizers.isEmpty()) {
            return query;
        }

        List<ESNode> queries = new ArrayList<>();

        normalizers.forEach(n -> {
            // We don't want the normalizer to apply to all fields
            // So we set the other fields as non-scoring
            List<ESBoost.Field> adjustedBoosts = fields.stream()
                    .map(f -> n.equals(f.normalizer()) ? f : ESBoost.Field.nonScoring(f.name()))
                    .toList();

            ESNode.TextQuery tq = query.withFields(adjustedBoosts);
            ESNode.Script script = getScript(query, n);

            queries.add(new ESNode.ScriptScore(tq, script));
        });

        // TODO: Naming, comment
        List<ESBoost.Field> noBoostForNormalized = fields.stream()
                .map(f -> f.normalizer() != null ? ESBoost.Field.nonScoring(f.name()) : f)
                .toList();
        queries.add(query.withFields(noBoostForNormalized));

        return queries.size() == 1 ? queries.getFirst() : new ESNode.Should(queries);
    }

    private static ESNode buildWithPhraseBoost(ESNode.TextQuery baseQuery, List<Token> tokens) {
        List<String> simplePhrases = getSimplePhrases(tokens);

        if (simplePhrases.isEmpty()) {
            return buildWithNormalizers(baseQuery);
        }

        ESBoost.TextQuerySettings settings = baseQuery.settings();

        int divisor = settings.phraseBoostDivisor();
        List<ESBoost.Field> dividedBoosts = baseQuery.boostFields()
                .stream()
                .map(f -> f.withBoost(f.boost() / divisor))
                .toList();

        List<ESNode> queries = new ArrayList<>();

        simplePhrases.forEach(s -> {
            // We can't use simple_query_string for phrase query
            ESNode.QueryString qs = new ESNode.QueryString(s, baseQuery.query().multiMatchType());
            ESNode.TextQuery q = new ESNode.TextQuery(qs, dividedBoosts, baseQuery.connective(), settings);
            queries.add(q);
        });

        queries.add(buildWithNormalizers(baseQuery));

        return new ESNode.Should(queries);
    }

    private static List<ESBoost.ScriptScoreNormalizer> collectNormalizers(List<ESBoost.Field> fields) {
        return fields.stream()
                .map(ESBoost.Field::normalizer)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
    }

    private static List<String> getSimplePhrases(List<Token> tokens) {
        List<String> simplePhrases = new ArrayList<>();
        List<String> currentSimpleSequence = new ArrayList<>();

        for (int i = 0; i < tokens.size(); i++) {
            Token token = tokens.get(i);
            if (!token.isQuoted() && !isMaskedOrTruncated(token.value())) {
                currentSimpleSequence.add(token.value());
            } else {
                if (currentSimpleSequence.size() > 1) {
                    simplePhrases.add(quote(String.join(" ", currentSimpleSequence)));
                }
                currentSimpleSequence.clear();
            }
            if (i == tokens.size() - 1 && currentSimpleSequence.size() > 1) {
                simplePhrases.add(quote(String.join(" ", currentSimpleSequence)));
            }
        }

        return simplePhrases;
    }

    private static ESNode.Script getScript(ESNode.TextQuery query, ESBoost.ScriptScoreNormalizer n) {
        String source = n.applyIf() == null ? n.function() : String.format("%s ? %s : _score", n.applyIf(), n.function());
        Map<String, Object> params = new HashMap<>();
        if ("length_normalizer".equals(n.name())) {
            String s = query.query().query();
            int qNumTokens = s.split("[\\s-]+").length;
            int lengthNormMultiplier = QueryUtil.isQuoted(s) ? qNumTokens : 1;
            params.put("q_num_tokens", qNumTokens);
            params.put("multiplier", lengthNormMultiplier);
        }
        return new ESNode.Script(source, params);
    }

    private static boolean isLikelyTextField(String field, ESMappings esMappings) {
        return !esMappings.hasKeywordSubfield(field)
                && !esMappings.hasFourDigitsKeywordField(field)
                && !esMappings.isDateTypeField(field)
                && !esMappings.isNestedTypeField(field)
                && !esMappings.isLongTypeField(field)
                && !esMappings.isKeywordTypeField(field);
    }

    private static boolean isNested(String field, ESMappings esMappings) {
        return getNestedStem(field, esMappings).isPresent();
    }

    public static Optional<String> getNestedStem(String field, ESMappings esMappings) {
        if (esMappings.isNestedTypeField(field)) {
            return Optional.of(field);
        }
        return esMappings.getNestedTypeFields().stream().filter(field::startsWith).findFirst();
    }

    // leading wildcards e.g. "*foo" are removed by simple_query_string
    private static final Pattern NON_SIMPLE_QUERY = Pattern.compile("\\\\[?]|([*?])\\S+");

    /**
     * Can this query string be handled by ES simple_query_string?
     */
    public static boolean isSimple(String queryString) {
        return !NON_SIMPLE_QUERY.matcher(queryString).find();
    }

    public static String escapeNonSimpleQueryString(String queryString) {
        // Treat escaped question marks as actual wildcards
        queryString = queryString.replace("\\?", "?");

        // The following chars are reserved in ES and need to be escaped to be used as literals: \+-=|&><!(){}[]^"~*?:/
        // Escape the ones that are not part of our query language.
        for (char c : List.of('=', '&', '!', '{', '}', '[', ']', '^', ':', '/')) {
            queryString = queryString.replace("" + c, "\\" + c);
        }

        // Inside words, treat '-' as regular hyphen instead of "NOT" and escape it
        queryString = queryString.replaceAll("(^|\\s+)-(\\S+)", "$1#ACTUAL_NOT#$2");
        queryString = queryString.replace("-", "\\-");
        queryString = queryString.replace("#ACTUAL_NOT#", "-");

        // Strip un-escapable characters
        for (char c : List.of('<', '>')) {
            queryString = queryString.replace("" + c, "");
        }

        return queryString;
    }

    private static boolean isMaskedOrTruncated(String s) {
        return !isSimple(s) || s.endsWith(Operator.WILDCARD);
    }
}
