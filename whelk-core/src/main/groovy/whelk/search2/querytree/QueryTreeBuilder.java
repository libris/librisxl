package whelk.search2.querytree;

import whelk.exception.InvalidQueryException;
import whelk.search2.Disambiguate;
import whelk.search2.Operator;
import whelk.search2.Query;
import whelk.search2.parse.Ast;
import whelk.search2.parse.Lex;
import whelk.search2.parse.Parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public class QueryTreeBuilder {
    public static Node buildTree(String queryString, Disambiguate disambiguate) throws InvalidQueryException {
        if (queryString.isEmpty()) {
            return new Any.EmptyString();
        } else if (queryString.equals(Operator.WILDCARD)) {
            return new Any.Wildcard();
        }
        return buildTree(getAst(queryString).tree, disambiguate, null, null, queryString);
    }

    private static Node buildTree(Ast.Node astNode, Disambiguate disambiguate, Selector selector, Operator operator, String q) throws InvalidQueryException {
        return switch (astNode) {
            case Ast.Group g -> buildFromGroup(g, disambiguate, selector, operator, q);
            case Ast.Not n -> buildFromNot(n, disambiguate, selector, operator, q);
            case Ast.Leaf l -> buildFromLeaf(l, disambiguate, selector, operator);
            case Ast.Code c -> buildFromCode(c, disambiguate, selector, q);
        };
    }

    private static Ast getAst(String queryString) throws InvalidQueryException {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString);
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols);
        return new Ast(parseTree);
    }

    private static Node buildFromGroup(Ast.Group group, Disambiguate disambiguate, Selector selector, Operator operator, String q) throws InvalidQueryException {
        if (group.operands().isEmpty()) {
            return selector != null
                    ? new Condition(selector, operator, new Any.EmptyGroup())
                    : new Any.EmptyGroup();
        }

        record MergedFreeText(List<Token> tokens, int insertAt) {}

        Property.TextQuery textQuery = disambiguate.getTextQueryProperty();

        List<Node> children = new ArrayList<>();
        Map<Selector, MergedFreeText> mergedFreeTextBySelector = new HashMap<>();

        Query.Connective connective = switch (group) {
            case Ast.And ignored -> Query.Connective.AND;
            case Ast.Or ignored -> Query.Connective.OR;
        };

        Predicate<FreeText> isMergeCompatible = ft -> ft.connective().equals(connective) || ft.tokens().size() < 2;

        for (int i = 0; i < group.operands().size(); i++) {
            int idx = i;
            Ast.Node operand = group.operands().get(i);
            Node builtNode = buildTree(operand, disambiguate, selector, operator, q);
            switch (builtNode) {
                case FreeText ft when isMergeCompatible.test(ft) ->
                        mergedFreeTextBySelector.computeIfAbsent(textQuery, k -> new MergedFreeText(new ArrayList<>(), idx))
                                .tokens()
                                .addAll(ft.tokens());
                case Condition c when c.selector().equals(selector) && c.value() instanceof FreeText ft && isMergeCompatible.test(ft) ->
                        mergedFreeTextBySelector.computeIfAbsent(c.selector(), k -> new MergedFreeText(new ArrayList<>(), idx))
                                .tokens()
                                .addAll(ft.tokens());
                default -> children.add(builtNode);
            }
        }

        mergedFreeTextBySelector.entrySet().stream()
                .sorted(Comparator.comparingInt(e -> e.getValue().insertAt()))
                .forEach(e -> {
                    Selector s = e.getKey();
                    List<Token> tokens = e.getValue().tokens();
                    int insertAt = e.getValue().insertAt();
                    FreeText ft = new FreeText(textQuery, tokens, connective);
                    Node n = s.equals(textQuery) ? ft : new Condition(s, operator, ft);
                    children.add(Math.min(insertAt, children.size()), n);
                });

        if (children.size() == 1) {
            return children.getFirst();
        }

        return switch (group) {
            case Ast.And ignored -> new And(children);
            case Ast.Or ignored -> new Or(children);
        };
    }

    private static Node buildFromNot(Ast.Not not, Disambiguate disambiguate, Selector selector, Operator operator, String q) throws InvalidQueryException {
        return buildTree(not.operand(), disambiguate, selector, operator, q).getInverse();
    }

    private static Node buildFromLeaf(Ast.Leaf leaf, Disambiguate disambiguate, Selector selector, Operator operator) throws InvalidQueryException {
        if (selector != null) {
            return buildCondition(selector, operator, leaf, disambiguate);
        }

        Lex.Symbol symbol = leaf.value();

        Optional<FilterAlias> filterAlias = disambiguate.mapToFilter(symbol.value());
        if (filterAlias.isPresent()) {
            var af = filterAlias.get();
            af.parse(disambiguate);
            return af;
        }

        return new FreeText(disambiguate.getTextQueryProperty(), getToken(symbol));
    }

    private static Node buildFromCode(Ast.Code c, Disambiguate disambiguate, Selector selector, String q) throws InvalidQueryException {
        if (selector != null) {
            // Nested selectors are not allowed, return the inner code segment as free text
            return new Condition(selector, c.operator(), asFreeText(c, q, disambiguate.getTextQueryProperty()));
        }
        selector = disambiguate.mapQueryKey(getToken(c.code()));
        return selector.isValid()
                ? buildTree(c.operand(), disambiguate, selector, c.operator(), q)
                : LegacyCodes.isQueryCode(c, selector)
                    ? LegacyCodes.build(c, disambiguate, selector)
                    : asFreeText(c, q, disambiguate.getTextQueryProperty()); // If the selector isn't valid, treat the whole segment as free text.
    }

    private static Condition buildCondition(Selector selector, Operator operator, Ast.Leaf leaf, Disambiguate disambiguate) {
        Token token = getToken(leaf.value());
        Value value = disambiguate.mapValueForSelector(selector, token).orElse(new FreeText(token));
        if (value instanceof Resource r && disambiguate.isRestrictedByValue(selector)) {
            selector = disambiguate.restrictByValue(selector, r.jsonForm());
        }
        Condition condition = new Condition(selector, operator, value);
        return condition.isTypeNode() ? condition.asTypeNode() : condition;
    }

    private static Token getToken(Lex.Symbol symbol) {
        return symbol.name() == Lex.TokenName.QUOTED_STRING
                ? new Token.Quoted(symbol.value(), symbol.offset() + 1)
                : new Token.Raw(symbol.value(), symbol.offset());
    }

    private static FreeText asFreeText(Ast.Code c, String q, Property.TextQuery textQuery) {
        int from = c.code().offset();
        int to = findSegmentEndIdx(c, q);
        String s = q.substring(from, to);
        return new FreeText(textQuery, new Token.Raw(s, from));
    }

    private static int findSegmentEndIdx(Ast.Code c, String q) {
        Rightmost rightmost = findRightmost(c, 0, q);
        var depth = rightmost.parenDepth();
        var rightmostSymbol = rightmost.symbol() != null ? rightmost.symbol() : c.code();
        var rightmostEnd = endIdx(rightmostSymbol);
        if (depth > 0) {
            int closing = findNthClosingParenthesis(q.substring(rightmostEnd), depth);
            if (closing == -1) {
                // Unreachable
                throw new IllegalStateException("Unbalanced parentheses after AST parsing");
            }
            return rightmostEnd + closing + 1;
        }
        return rightmostEnd;
    }

    private static int endIdx(Lex.Symbol symbol) {
        return symbol.offset() + symbol.value().length() + (symbol.isQuoted() ? 2 : 0);
    }

    private record Rightmost(Lex.Symbol symbol, int parenDepth) {}

    private static Rightmost findRightmost(Ast.Node n, int parenDepth, String q) {
        return switch (n) {
            case Ast.Group g -> {
                int nextDepth = parenDepth + 1;
                yield g.operands().isEmpty()
                        ? new Rightmost(null, nextDepth)
                        : findRightmost(g.operands().getLast(), nextDepth, q);
            }
            case Ast.Code c -> {
                int nextDepth = parenDepth + (isAtomicWrappedInParentheses(c.operand(), q) ? 1 : 0);
                yield findRightmost(c.operand(), nextDepth, q);
            }
            case Ast.Leaf l -> new Rightmost(l.value(), parenDepth);
            case Ast.Not not -> findRightmost(not.operand(), parenDepth, q);
        };
    }

    private static boolean isAtomicWrappedInParentheses(Ast.Node operand, String q) {
        return switch (operand) {
            // We only need to check for an opening parenthesis since unbalanced parentheses won't pass AST parsing
            case Ast.Leaf l -> isPrecededByOpeningParen(l.value(), q);
            case Ast.Code c -> isPrecededByOpeningParen(c.code(), q);
            default -> false;
        };
    }

    private static boolean isPrecededByOpeningParen(Lex.Symbol symbol, String q) {
        int start = symbol.offset();

        for (int i = start - 1; i >= 0; i--) {
            var c = q.charAt(i);
            if (Character.isWhitespace(c)) {
                continue;
            }
            return c == '(';
        }

        return false;
    }

    private static int findNthClosingParenthesis(String s, int n) {
        int count = 0;

        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == ')') {
                if (++count == n) {
                    return i;
                }
            }
        }

        return -1;
    }

    /**
     * Hardcoded handling of some legacy query codes that are used by xsearch clients
     */
    private static final class LegacyCodes {
        enum LegacyCode {
            /** "Specialindex för maskinell gruppering, namngivning och behandling av materialtyper (bok, tidskrift, e-bok, bild etc.)." */
            MAT,

            /** "Kod för bibliografisk nivå / publikationstyp MARC 000/07" */
            BIBN,

            /** "Sekundärt materialtypsindex. Kompletterande materialtyper på mer detaljerad nivå. Varje katalogpost kan ha 0, 1 eller flera sådana sekundära typer" */
            MTAG,

            /** "Kod för trunkerad sökning på Deweyklassifikation" */
            DDCT
        }

        static final List<String> CODES = Arrays.stream(LegacyCode.values()).map(LegacyCode::toString).toList();

        static final String BOOK = "workType:Monograph instanceCategory:\"https://id.kb.se/term/rda/Volume\"";
        static final String NOTATED_MUSIC = "workCategory:\"https://id.kb.se/term/rda/NotatedMusic\"";
        static final String MONOGRAPH = "workType:Monograph";
        static final String SERIAL = "workType:Serial";
        static final String COLLECTION = "workType:Collection";
        static final String INTEGRATING = "workType:Integrating";

        static boolean isQueryCode(Ast.Code c, Selector selector) {
            if (c.operator() != Operator.EQUALS) {
                return false;
            }

            return CODES.contains(selector.queryKey().toUpperCase());
        }

        static Node build(Ast.Code c, Disambiguate disambiguate, Selector selector) throws InvalidQueryException {
            var code = LegacyCode.valueOf(selector.queryKey().toUpperCase());

            return switch (c.operand()) {
                // webbsök treats mat:(barn skol) as barn OR skol
                case Ast.Group g -> new Or(g.operands().stream().map(n -> {
                    try {
                        return build(code, n, disambiguate);
                    } catch (InvalidQueryException e) {
                        throw new RuntimeException(e);
                    }
                }).toList());
                case Ast.Leaf l -> build(code, l, disambiguate);
                default -> throw new InvalidQueryException("Could not handle:" + c);
            };
        }

        static Node build(LegacyCode code, Ast.Node n, Disambiguate disambiguate) throws InvalidQueryException {
            if (n instanceof Ast.Leaf leaf) {
                return build(code, leaf, disambiguate);
            }
            throw new RuntimeException("Could not handle: " + code + ": " + n);
        }

        static Node build(LegacyCode code, Ast.Leaf leaf, Disambiguate disambiguate) throws InvalidQueryException {
            var value = leaf.value().value();

            var mappedQuery = switch(code) {
                case MAT -> switch (value) {
                    case "bok",
                         "böcker",
                         "book",
                         "books" -> BOOK;

                    case "seriell publikation",
                         "seriella publikationer",
                         "serial",
                         "serials" -> SERIAL;

                    case "noter" -> NOTATED_MUSIC;

                    // TODO? very little use: ebook, barn, skol, bokannat
                    // seen in queries but gives no result: art, kon, kap, sam, foto, dok

                    default -> value;
                };

                case BIBN -> switch (value) {
                    case "m" -> MONOGRAPH;
                    case "s" -> SERIAL;
                    case "c" -> COLLECTION;
                    case "i" -> INTEGRATING;
                    default -> value;
                };

                case MTAG -> switch (value) {
                    case "free" -> "freeOnline"; // the only one seen in logs
                    default -> value;
                };

                case DDCT -> String.format("ddc:%s%s", value, Operator.WILDCARD);
            };

            return buildTree(mappedQuery, disambiguate);
        }
    }
}
