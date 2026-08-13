package whelk.search2.esquery;

import whelk.search.QueryDateTime;
import whelk.search2.ESSettings;
import whelk.search2.EsMappings;
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
import static whelk.search2.EsMappings.FOUR_DIGITS_KEYWORD_SUFFIX;
import static whelk.search2.EsMappings.FOUR_DIGITS_SHORT_SUFFIX;
import static whelk.search2.EsMappings.KEYWORD;
import static whelk.search2.QueryUtil.quote;

public class EsQueryTreeBuilder {
    public static EsQuery buildFrom(QueryTree queryTree, ESSettings settings) {
        return buildFrom(queryTree.tree(), settings);
    }

    public static EsQuery buildFrom(Node queryTreeNode, ESSettings settings) {
        return switch (queryTreeNode) {
            case Condition c -> buildFromCondition(c, settings);
            case FilterAlias fa -> buildFrom(fa.getParsed(), settings);
            case And and -> buildFromAnd(and, settings);
            case Or or -> buildFromOr(or, settings);
            case Not not -> buildFromNot(not, settings);
        };
    }

    private static EsQuery buildFromNot(Not not, ESSettings esSettings) {
        return new EsQuery.MustNot(buildFrom(not.node(), esSettings));
    }

    private static EsQuery buildFromOr(Or or, ESSettings esSettings) {
        List<EsQuery> subQueries = or.children().stream()
                .map(n -> buildFrom(n, esSettings))
                .toList();

        return or instanceof QueryTree.ExpandedTree.DerivedOr derivedOr
                ? buildDerivedDisjunction(derivedOr.originalCondition(), subQueries)
                : factorOutNested(new EsQuery.Should(subQueries));
    }

    private static EsQuery buildDerivedDisjunction(Condition origCondition, List<EsQuery> subQueries) {
        // TODO: Explaining comments
        EsQuery.DisMax disMax = new EsQuery.DisMax(subQueries);
        EsQuery query = factorOutNested(disMax);

        boolean shouldCombineFields = origCondition.selector().isComposite();

        if (shouldCombineFields) {
            return query instanceof EsQuery.Nested(EsQuery.DisMax dm,
                                                   EsQuery.NestedStem stem,
                                                   Set<EsQuery.NestedField> fields)
                    ? new EsQuery.Nested(combineFields(dm), stem, fields)
                    : combineFields(disMax);
        }

        return query;
    }

    private static EsQuery factorOutNested(EsQuery.Disjunction dis) {
        boolean allSubQueriesAreNested = dis.subQueries().stream().allMatch(EsQuery.Nested.class::isInstance);
        if (!allSubQueriesAreNested) {
            return dis;
        }

        List<EsQuery.Nested> nestedSubQueries = dis.subQueries().stream()
                .map(EsQuery.Nested.class::cast)
                .toList();

        boolean differentStems = nestedSubQueries.stream().map(EsQuery.Nested::stem).distinct().count() > 1;
        if (differentStems) {
            return dis;
        }

        List<EsQuery> subQueriesAsNonNested = nestedSubQueries.stream()
                .map(EsQuery.Nested::query)
                .toList();
        EsQuery.NestedStem stem = nestedSubQueries.getFirst().stem();
        Set<EsQuery.NestedField> nestedFields = nestedSubQueries.stream()
                .map(EsQuery.Nested::fields)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        return new EsQuery.Nested(dis.withSubQueries(subQueriesAsNonNested), stem, nestedFields);
    }

    private static EsQuery combineFields(EsQuery.Disjunction dis) {
        List<EsQuery> subQueries = combineSimpleTextQueryFields(dis.subQueries());
        return subQueries.size() == 1
                ? subQueries.getFirst()
                : dis.withSubQueries(subQueries);
    }

    private static List<EsQuery> combineSimpleTextQueryFields(List<EsQuery> subQueries) {
        List<EsQuery.TextQuery> simpleQueries = subQueries.stream()
                .filter(subQuery -> subQuery instanceof EsQuery.TextQuery t && t.isSimple())
                .map(EsQuery.TextQuery.class::cast)
                .toList();

        if (simpleQueries.size() < 2) {
            return subQueries;
        }

        List<EsBoost.Field> mergedFields = simpleQueries.stream()
                .flatMap(q -> q.boostFields().stream())
                .toList();

        EsQuery.TextQuery first = simpleQueries.getFirst();
        int firstIdx = subQueries.indexOf(first);

        List<EsQuery> result = new ArrayList<>(subQueries);
        result.removeAll(simpleQueries);
        result.add(firstIdx, first.withFields(mergedFields));

        return result;
    }

    private static EsQuery buildFromAnd(And and, ESSettings esSettings) {
        List<EsQuery> subQueries = and.children().stream()
                .map(n -> buildFrom(n, esSettings))
                .toList();
        EsQuery.Must must = new EsQuery.Must(subQueries);
        return groupNested(must);
    }

    private static EsQuery groupNested(EsQuery.Must must) {
        List<EsQuery> subQueries = new ArrayList<>();

        AtomicReference<EsQuery.NestedStem> currentNestedStem = new AtomicReference<>();
        List<EsQuery.Nested> currentGroup = new ArrayList<>();

        Runnable collectNested = () -> {
            if (currentGroup.size() == 1) {
                subQueries.add(currentGroup.getFirst());
            }
            else if (currentGroup.size() > 1){
                List<EsQuery> subQueriesAsNonNested = currentGroup.stream()
                        .map(EsQuery.Nested::query)
                        .toList();
                Set<EsQuery.NestedField> nestedFields = currentGroup.stream()
                        .map(EsQuery.Nested::fields)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
                EsQuery.Nested outerNested = new EsQuery.Nested(new EsQuery.Must(subQueriesAsNonNested),
                        currentNestedStem.get(),
                        nestedFields);
                subQueries.add(outerNested);
            }

            currentNestedStem.set(null);
            currentGroup.clear();
        };

        Predicate<EsQuery.Nested> isCompatibleWithCurrentGroup = n -> {
            EsQuery.NestedStem stem = n.stem();
            Set<EsQuery.NestedField> fields = n.fields();
            Set<EsQuery.NestedField> currentFields = currentGroup.stream()
                    .map(EsQuery.Nested::fields)
                    .flatMap(Set::stream)
                    .collect(Collectors.toSet());
            return stem.equals(currentNestedStem.get())
                    && fields.stream().noneMatch(f -> !f.isRepeatable() && currentFields.contains(f));
        };

        for (EsQuery subQuery : must.subQueries()) {
            if (subQuery instanceof EsQuery.Nested nested) {
                if (isCompatibleWithCurrentGroup.test(nested)) {
                    currentGroup.add(nested);
                } else {
                    // Collect previous group
                    collectNested.run();
                    // Then begin new group
                    currentNestedStem.set(nested.stem());
                    currentGroup.add(nested);
                }
            } else if (subQuery instanceof EsQuery.MustNot(EsQuery.Nested nested)
                    // A must_not(nested(...)) clause may only join an existing nested group if that group
                    // already contains a "positive" nested clause for the same path.
                    && isCompatibleWithCurrentGroup.test(nested)) {
                // Move the negation inside the nested query so it can be grouped with the existing nested clauses
                EsQuery.MustNot nonNested = new EsQuery.MustNot(nested.query());
                EsQuery.Nested outerNested = new EsQuery.Nested(nonNested, nested.stem(), Set.of());
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
                : new EsQuery.Must(subQueries);
    }

    private static EsQuery buildFromCondition(Condition c, ESSettings esSettings) {
        if (c.isAnyQuery()) {
            return new EsQuery.MatchAll();
        }
        if (c.isTextQuery()) {
            return buildFromFreeText(c.freeTextValue(), esSettings.boost());
        }

        String f = c.selector().esField();
        Operator op = c.operator();
        Value v = c.value();

        EsQuery query = switch (v) {
            case Any ignored -> new EsQuery.Exists(f);
            case DateTime dateTime -> buildFromDateTimeValue(f, op, dateTime, esSettings);
            case FreeText freeText -> buildFromFreeTextValue(f, op, freeText, esSettings, c.selector().isObjectProperty());
            case InvalidValue ignored -> new EsQuery.MatchNone();
            case Link link -> buildFromLinkValue(f, op, link, esSettings);
            case VocabTerm vocabTerm -> new EsQuery.TermQuery(f, vocabTerm.jsonForm());
            case Term term -> new EsQuery.TermQuery(f, term.term());
            case YearRange yearRange -> buildFromYearRangeValue(f, op, yearRange, esSettings);
        };

        EsMappings esMappings = esSettings.mappings();

        return getNestedStem(f, esMappings)
                .map(stem -> {
                    boolean includeInParent = esMappings.isNestedIncludeInParentField(stem);
                    boolean isRepeatable = c.selector().isLdSetContainer();
                    EsQuery.NestedStem nestedStem = new EsQuery.NestedStem(stem, includeInParent);
                    EsQuery.NestedField nestedField = new EsQuery.NestedField(f, isRepeatable);
                    return new EsQuery.Nested(query, nestedStem, Set.of(nestedField));
                })
                .map(EsQuery.class::cast)
                .orElse(query);
    }

    private static EsQuery buildFromFreeText(FreeText ft, EsBoost esBoost) {
        return buildTextQuery(ft, esBoost.freeTextFields(), esBoost.freeTextQuerySettings());
    }

    private static EsQuery buildFromDateTimeValue(String field, Operator operator, DateTime dateTime, ESSettings esSettings) {
        if (esSettings.mappings().isDateTypeField(field)) {
            return switch (operator) {
                case EQUALS -> new EsQuery.TermQuery(field, dateTime.dateTime().toElasticDateString());
                case GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQUALS, GREATER_THAN_OR_EQUALS -> new EsQuery.RangeQuery(field, Map.of(operator, dateTime.dateTime().toElasticDateString()));
                case LIKE -> new EsQuery.MatchNone(); // Makes no sense
            };
        }
        return buildFieldedTextQuery(field, new FreeText(dateTime.toString()), esSettings);
    }

    private static EsQuery buildFromFreeTextValue(String field, Operator operator, FreeText ft, ESSettings esSettings, boolean addSearchKey) {
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
                    yield new EsQuery.RangeQuery(digitsField, Map.of(operator, limit));
                } else {
                    yield new EsQuery.MatchNone();
                }
            }
        };
    }

    private static EsQuery buildFromLinkValue(String field, Operator operator, Link link, ESSettings esSettings) {
        return operator == Operator.LIKE && !"".equals(link.getNeedle())
                ? buildWithNeedle(field, link, esSettings.mappings())
                : new EsQuery.TermQuery(String.format("%s.%s", field, ID_KEY), link.jsonForm());
    }

    private static EsQuery buildWithNeedle(String field, Link link, EsMappings esMappings) {
        String needle = Arrays.stream(link.getNeedle().split("\\s"))
                .map(QueryUtil::quote)
                .collect(Collectors.joining(" "));

        String idField = String.format("%s.%s", field, ID_KEY);
        String strField = String.format("%s.%s", field, SEARCH_KEY);

        EsQuery idQuery = new EsQuery.TermQuery(idField, link.jsonForm());
        EsQuery textQuery = EsQuery.TextQuery.simpleUnboostedQuery(needle, strField);

        EsQuery notLinked = isNested(field, esMappings)
                ? new EsQuery.MustNot(new EsQuery.Exists(idField))
                : new EsQuery.MustNot(idQuery);
        EsQuery.Must blankQuery = new EsQuery.Must(List.of(textQuery, notLinked));

        float linkedBeforeBlank = 50_000f;
        EsQuery boostedIdQuery = new EsQuery.ConstantScore(idQuery, linkedBeforeBlank);

        return new EsQuery.Should(List.of(boostedIdQuery, blankQuery));
    }

    private static EsQuery buildPerTokenTermQueriesQuery(String field, FreeText ft, ESSettings esSettings) {
        EsMappings mappings = esSettings.mappings();

        // Known placeholder values (0000, 9999) are excluded from 4-digit fields to prevent them from being treated as valid years in sorting and aggregations.
        Predicate<String> isFourDigitsFieldValue = s -> s.length() == 4 && !s.equals("0000") && !s.equals("9999");

        List<EsQuery> perTokenTermQueries = new ArrayList<>();

        for (Token t : ft.tokens()) {
            String v = t.value();
            if (mappings.hasFourDigitsKeywordField(field) && t.isDigits() && isFourDigitsFieldValue.test(v)) {
                perTokenTermQueries.add(new EsQuery.TermQuery(field + FOUR_DIGITS_KEYWORD_SUFFIX, v));
            } else if (mappings.hasKeywordSubfield(field) && !isMaskedOrTruncated(v)) {
                perTokenTermQueries.add(new EsQuery.TermQuery(String.format("%s.%s", field, KEYWORD), v));
            } else if (mappings.isLongTypeField(field) && t.isDigits()) {
                perTokenTermQueries.add(new EsQuery.TermQuery(field, Long.parseLong(v)));
            } else {
                return buildFieldedTextQuery(field, ft, esSettings);
            }
        }

        if (perTokenTermQueries.size() == 1) {
            return perTokenTermQueries.getFirst();
        }

        return switch (ft.connective()) {
            case OR -> new EsQuery.Should(perTokenTermQueries);
            case AND -> new EsQuery.Must(perTokenTermQueries);
        };
    }

    private static EsQuery buildFromYearRangeValue(String field, Operator operator, YearRange yearRange, ESSettings esSettings) {
        if (operator == Operator.EQUALS) {
            if (esSettings.mappings().hasFourDigitsShortField(field)) {
                return buildFromYearRangeValue(field + FOUR_DIGITS_SHORT_SUFFIX, yearRange, Integer::parseInt);
            } else if (esSettings.mappings().isDateTypeField(field)) {
                return buildFromYearRangeValue(field, yearRange, v -> QueryDateTime.parse(v).toElasticDateString());
            } else {
                return buildFieldedTextQuery(field, new FreeText(yearRange.toString()), esSettings);
            }
        }

        return new EsQuery.MatchNone(); // Makes no sense
    }

    private static EsQuery buildFromYearRangeValue(String field, YearRange yearRange, Function<String, Object> parseLimit) {
        Map<Operator, Object> rangeMap = new HashMap<>();

        if (!yearRange.min().isEmpty()) {
            rangeMap.put(Operator.GREATER_THAN_OR_EQUALS, parseLimit.apply(yearRange.min()));
        }
        if (!yearRange.max().isEmpty()) {
            rangeMap.put(Operator.LESS_THAN_OR_EQUALS, parseLimit.apply(yearRange.max()));
        }

        return new EsQuery.RangeQuery(field, rangeMap);
    }

    private static EsQuery buildFieldedTextQuery(String f, FreeText ft, ESSettings esSettings) {
        EsBoost.FieldedQuerySettings boostSettings = esSettings.boost().fieldedQuerySettings();
        EsBoost.Field field = new EsBoost.Field(f, boostSettings.defaultBoostFactor());
        return buildTextQuery(ft, List.of(field), boostSettings);
    }

    private static EsQuery buildTextQuery(FreeText ft, List<EsBoost.Field> fields, EsBoost.TextQuerySettings boostSettings) {
        String s = ft.toEsString();
        s = Unicode.normalizeForSearch(s);

        // TODO search for original string OR stripped string?
        if (Unicode.looksLikeIsbn(s) && s.contains("-")) {
            s = s.replace("-", "");
        }

        EsQuery.TextQueryMode textQueryMode = isSimple(s)
                ? new EsQuery.SimpleQueryString(s)
                : new EsQuery.QueryString(escapeNonSimpleQueryString(s), EsQuery.MultiMatchType.most_fields);

        EsQuery.TextQuery baseQuery = new EsQuery.TextQuery(textQueryMode, fields, ft.connective(), boostSettings);

        if (boostSettings.boostPhrase()) {
            return buildWithPhraseBoost(baseQuery, ft.tokens());
        }

        return buildWithNormalizers(baseQuery);
    }

    private static EsQuery buildWithNormalizers(EsQuery.TextQuery query) {
        List<EsBoost.Field> fields = query.boostFields();
        List<EsBoost.ScriptScoreNormalizer> normalizers = collectNormalizers(fields);

        if (normalizers.isEmpty()) {
            return query;
        }

        List<EsQuery> queries = new ArrayList<>();

        normalizers.forEach(n -> {
            // We don't want the normalizer to apply to all fields
            // So we set the other fields as non-scoring
            List<EsBoost.Field> adjustedBoosts = fields.stream()
                    .map(f -> n.equals(f.normalizer()) ? f : EsBoost.Field.nonScoring(f.name()))
                    .toList();

            EsQuery.TextQuery tq = query.withFields(adjustedBoosts);
            EsQuery.Script script = getScript(query, n);

            queries.add(new EsQuery.ScriptScore(tq, script));
        });

        // TODO: Naming, comment
        List<EsBoost.Field> noBoostForNormalized = fields.stream()
                .map(f -> f.normalizer() != null ? EsBoost.Field.nonScoring(f.name()) : f)
                .toList();
        queries.add(query.withFields(noBoostForNormalized));

        return queries.size() == 1 ? queries.getFirst() : new EsQuery.Should(queries);
    }

    private static EsQuery buildWithPhraseBoost(EsQuery.TextQuery baseQuery, List<Token> tokens) {
        List<String> simplePhrases = getSimplePhrases(tokens);

        if (simplePhrases.isEmpty()) {
            return buildWithNormalizers(baseQuery);
        }

        EsBoost.TextQuerySettings settings = baseQuery.settings();

        int divisor = settings.phraseBoostDivisor();
        List<EsBoost.Field> dividedBoosts = baseQuery.boostFields()
                .stream()
                .map(f -> f.withBoost(f.boost() / divisor))
                .toList();

        List<EsQuery> queries = new ArrayList<>();

        simplePhrases.forEach(s -> {
            // We can't use simple_query_string for phrase query
            EsQuery.QueryString qs = new EsQuery.QueryString(s, baseQuery.query().multiMatchType());
            EsQuery.TextQuery q = new EsQuery.TextQuery(qs, dividedBoosts, baseQuery.connective(), settings);
            queries.add(q);
        });

        queries.add(buildWithNormalizers(baseQuery));

        return new EsQuery.Should(queries);
    }

    private static List<EsBoost.ScriptScoreNormalizer> collectNormalizers(List<EsBoost.Field> fields) {
        return fields.stream()
                .map(EsBoost.Field::normalizer)
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

    private static EsQuery.Script getScript(EsQuery.TextQuery query, EsBoost.ScriptScoreNormalizer n) {
        String source = n.applyIf() == null ? n.function() : String.format("%s ? %s : _score", n.applyIf(), n.function());
        Map<String, Object> params = new HashMap<>();
        if ("length_normalizer".equals(n.name())) {
            String s = query.query().query();
            int qNumTokens = s.split("[\\s-]+").length;
            int lengthNormMultiplier = QueryUtil.isQuoted(s) ? qNumTokens : 1;
            params.put("q_num_tokens", qNumTokens);
            params.put("multiplier", lengthNormMultiplier);
        }
        return new EsQuery.Script(source, params);
    }

    private static boolean isLikelyTextField(String field, EsMappings esMappings) {
        return !esMappings.hasKeywordSubfield(field)
                && !esMappings.hasFourDigitsKeywordField(field)
                && !esMappings.isDateTypeField(field)
                && !esMappings.isNestedTypeField(field)
                && !esMappings.isLongTypeField(field)
                && !esMappings.isKeywordTypeField(field);
    }

    private static boolean isNested(String field, EsMappings esMappings) {
        return getNestedStem(field, esMappings).isPresent();
    }

    public static Optional<String> getNestedStem(String field, EsMappings esMappings) {
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
