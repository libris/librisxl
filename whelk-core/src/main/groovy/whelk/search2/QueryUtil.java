package whelk.search2;

import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import whelk.JsonLd;
import whelk.Whelk;
import whelk.search.ESQueryLensBoost;
import whelk.search2.querytree.QueryTree;
import whelk.util.DocumentUtil;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Arrays;

import static whelk.component.ElasticSearch.flattenedLangMapKey;

public class QueryUtil {
    private static final Escaper QUERY_ESCAPER = UrlEscapers.urlFormParameterEscaper();

    private final Whelk whelk;
    public final EsMappings esMappings;
    public final ESQueryLensBoost lensBoost;
    public List<String> boostedFields;

    public QueryUtil(Whelk whelk) {
        this.whelk = whelk;
        this.esMappings = new EsMappings(whelk.elastic != null ? whelk.elastic.getMappings() : Collections.emptyMap());
        this.lensBoost = new ESQueryLensBoost(whelk.getJsonld());
    }

    public Map<?, ?> query(Map<String, Object> queryDsl) {
        //System.out.println(queryDsl.toString());
        return whelk.elastic.query(queryDsl);
    }

    public boolean esIsConfigured() {
        return whelk != null && whelk.elastic != null;
    }

    public int maxItems() {
        return whelk.elastic.maxResultWindow;
    }

    public static String quoteIfPhraseOrContainsSpecialSymbol(String s) {
        // TODO: Don't hardcode
        return s.matches(".*(>=|<=|[=!~<>(): ]).*") ? "\"" + s + "\"" : s;
    }

    public static String encodeUri(String uri) {
        String decoded = URLDecoder.decode(uri.replace("+", "%2B"), StandardCharsets.UTF_8);
        return escapeQueryParam(decoded)
                .replace("%23", "#")
                .replace("+", "%20");
    }

    public static Map<String, Object> castToStringObjectMap(Object o) {
        return ((Map<?, ?>) o).entrySet()
                .stream()
                .collect(Collectors.toMap(e -> (String) e.getKey(), e -> (Object) e.getValue()));
    }

    public Optional<Map<?, ?>> loadThing(String id) {
        return loadThing(id, whelk);
    }

    public static Optional<Map<?, ?>> loadThing(String id, Whelk whelk) {
        return Optional.ofNullable(whelk.loadData(id))
                .map(data -> data.get(JsonLd.GRAPH_KEY))
                .map(graph -> (Map<?, ?>) ((List<?>) graph).get(1));
    }

    public static String makeFindUrl(String i, String q, Map<String, String> nonQueryParams) {
        return makeFindUrl(i, q, makeParams(nonQueryParams));
    }

    public static String makeFindUrl(QueryTree qt, Map<String, String> nonQueryParams) {
        return makeFindUrl(qt.getTopLevelFreeText(), qt.toString(), nonQueryParams);
    }

    public static String makeFindUrl(String i, String q, List<String> nonQueryParams) {
        List<String> params = new ArrayList<>();
        params.add(makeParam(QueryParams.ApiParams.SIMPLE_FREETEXT, i));
        params.add(makeParam(QueryParams.ApiParams.QUERY, q));
        params.addAll(nonQueryParams);
        return makeFindUrl(params);
    }

    public static String makeFindUrl(List<String> params) {
        return "/find?" + String.join("&", params);
    }

    public static List<String> makeParams(Map<String, String> params) {
        return params.entrySet()
                .stream()
                .map(entry -> makeParam(entry.getKey(), entry.getValue()))
                .toList();
    }

    private static String makeParam(String key, String value) {
        return String.format("%s=%s", escapeQueryParam(key), escapeQueryParam(value));
    }

    private static String escapeQueryParam(String input) {
        return QUERY_ESCAPER.escape(input)
                // We want pretty URIs, restore some characters which are inside query strings
                // https://tools.ietf.org/html/rfc3986#section-3.4
                .replace("%3A", ":")
                .replace("%2F", "/")
                .replace("%40", "@");
    }

    public Optional<String> getNestedPath(String path) {
        if (esMappings.isNestedField(path)) {
            return Optional.of(path);
        }
        return esMappings.getNestedFields().stream().filter(path::startsWith).findFirst();
    }

    public static Map<String, Object> mustWrap(Object l) {
        return boolWrap(Map.of("must", l));
    }

    public static Map<String, Object> mustNotWrap(Object o) {
        return boolWrap(Map.of("must_not", o));
    }

    public static Map<String, Object> shouldWrap(List<?> l) {
        return boolWrap(Map.of("should", l));
    }

    public static Map<String, Object> boolWrap(Map<?, ?> m) {
        return Map.of("bool", m);
    }

    public static Map<String, Object> nestedWrap(String nestedPath, Map<String, Object> query) {
        return Map.of("nested", Map.of("path", nestedPath, "query", query));
    }

    public String getSortField(String termPath) {
        var path = expandLangMapKeys(termPath);
        if (esMappings.isKeywordField(path) && !esMappings.isFourDigitField(path)) {
            return String.format("%s.keyword", path);
        } else {
            return termPath;
        }
    }

    private String expandLangMapKeys(String field) {
        var parts = field.split("\\.");
        if (parts.length > 0) {
            assert whelk != null;
            var lastIx = parts.length - 1;
            if (whelk.getJsonld().langContainerAlias.containsKey(parts[lastIx])) {
                parts[lastIx] = flattenedLangMapKey(parts[lastIx]);
                return String.join(".", parts);
            }
        }
        return field;
    }

    @SuppressWarnings("unchecked")
    public Function<Map<String, Object>, Map<String, Object>> getApplyLensFunc(QueryParams queryParams) {
        return framedThing -> {
            @SuppressWarnings("rawtypes")
            List<List> preservedPaths = queryParams.object != null
                    ? JsonLd.findPaths(framedThing, "@id", queryParams.object)
                    : Collections.emptyList();

            return switch (queryParams.lens) {
                case "chips" -> (Map<String, Object>) whelk.getJsonld().toChip(framedThing, preservedPaths);
                case "full" -> removeSystemInternalProperties(framedThing);
                default -> whelk.getJsonld().toCard(framedThing, false, false, false, preservedPaths, true);
            };
        };
    }

    private static Map<String, Object> removeSystemInternalProperties(Map<String, Object> framedThing) {
        DocumentUtil.traverse(framedThing, (value, path) -> {
            if (!path.isEmpty() && ((String) path.getLast()).startsWith("_")) {
                return new DocumentUtil.Remove();
            }
            return DocumentUtil.NOP;
        });
        return framedThing;
    }

//@CompileStatic(TypeCheckingMode.SKIP)
public void boostFields(Map<String, String[]> ogQueryParameters) {
	if ( ogQueryParameters == null ) {
		return;
	}

        Map<String, String[]> queryParameters = new HashMap<>(ogQueryParameters);

	String[] originalTypeParam = queryParameters.get("@type");
        if (originalTypeParam != null) {
            queryParameters.put("@type", expandTypeParam(originalTypeParam, whelk.getJsonld()));
        }

	String[] boostParam = queryParameters.get("_boost");

        String boostMode = boostParam != null ? boostParam[0] : null;
        this.boostedFields = getBoostFields(originalTypeParam, boostMode);
}

public List<String> tokenize(String tokens, String delimiter) {
	List<String> tokenized = new ArrayList<String>();

	if ( tokens != null ) {
		for (String s: tokens.split(delimiter)) {
			if ( s != "" ) {
				tokenized.add(s);
			}
		}
	}
	return tokenized;
}

private List<String> getBoostFields(String[] types, String boostMode) {
	if ( boostMode == null ) {
		boostMode = "";
	}
        if (boostMode != "" && boostMode.indexOf("^") > -1) {
            //return boostMode.tokenize(",");
		return tokenize(boostMode, ",");
        }
        if (boostMode.equals("id.kb.se")) {
            return CONCEPT_BOOST;
        }

	Map<String, List<String>> boostFieldsByType = new HashMap<>();

        String typeKey = "";
        //String typeKey = types != null ? types.toUnique().sort().join(',') : '';
	if ( types != null ) {
		List<String> sorted_unique = Arrays.asList(types).stream().sorted().distinct().collect(Collectors.toList());
		typeKey = String.join(",", sorted_unique);
	}

        typeKey += boostMode;

        List<String> boostFields = boostFieldsByType.get(typeKey);
        if (boostFields == null) {
            if (boostMode.equals("hardcoded")) {
                boostFields = new ArrayList<String>(Arrays.asList(
                    "prefLabel^100",
                    "code^100",
                    "name^100",
                    "familyName^100", "givenName^100",
                    "lifeSpan^100", "birthYear^100", "deathYear^100",
                    "hasTitle.mainTitle^100", "title^100",
                    "heldBy.sigel^100"
                ));
            } else {
                boostFields = computeBoostFields(types);
            }
            boostFieldsByType.put(typeKey, boostFields);
        }
	return boostFields;
}

private List<String> computeBoostFields(String[] types) {
        /* FIXME:
           lensBoost.computeBoostFieldsFromLenses does not give a good result for Concept. 
           Use hand-tuned boosting instead until we improve boosting/ranking in general. See LXL-3399 for details. 
        */
        // def l = ((types ?: []) as List<String>).split { jsonld.isSubClassOf(it, 'Concept') }

	List<String> conceptTypes = new ArrayList<>();
	List<String> otherTypes = new ArrayList<>();
	if ( types != null ) {
		for(String s: types){
			if (whelk.getJsonld().isSubClassOf(s, "Concept")) {
				conceptTypes.add(s);
			}
			else {
				otherTypes.add(s);
			}
		}
	}
	
        //def (conceptTypes, otherTypes) = [l[0], l[1]]
        
        if (!conceptTypes.isEmpty()) {
            if (!otherTypes.isEmpty()) {
                //def fromLens = this.lensBoost.computeBoostFieldsFromLenses(otherTypes as String[]);

                List<String> fromLens = this.lensBoost.computeBoostFieldsFromLenses(otherTypes.toArray(new String[0]));

                //def conceptFields = CONCEPT_BOOST.collect{ it.split('\\^')[0]};
                List<String> conceptFields = CONCEPT_BOOST.stream().map( s -> s.split("\\^")[0] ).collect(Collectors.toList());

                //def otherFieldsBoost = fromLens.findAll{!conceptFields.contains(it.split('\\^')[0]); }

                List<String> otherFieldsBoost = new ArrayList<>();
		for(String s: fromLens){
			if (!(conceptFields.contains(s.split("\\^")[0]))){
				CONCEPT_BOOST.add(s);
			}
		} 

                //return CONCEPT_BOOST + otherFieldsBoost;

                return CONCEPT_BOOST;
            }
            else {
                return CONCEPT_BOOST;
            }
        }
        else {
            return this.lensBoost.computeBoostFieldsFromLenses(types);
        }
}
        
private static final List<String> CONCEPT_BOOST = new ArrayList<String>(Arrays.asList(
            "prefLabel^1500",
            "prefLabelByLang.sv^1500",
            "label^500",
            "labelByLang.sv^500",
            "code^200",
            "termComponentList._str.exact^125",
            "termComponentList._str^75",
            "altLabel^150",
            "altLabelByLang.sv^150",
            "hasVariant.prefLabel.exact^150",
            "_str.exact^100",
            "inScheme._str.exact^100",
            "inScheme._str^100",
            "inCollection._str.exact^10",
            "broader._str.exact^10",
            "exactMatch._str.exact^10",
            "closeMatch._str.exact^10",
            "broadMatch._str.exact^10",
            "related._str.exact^10",
            "scopeNote^10",
            "keyword._str.exact^10"
));

/**
     * Expand `@type` query parameter with subclasses.
     *
     * This also removes superclasses, since we only care about the most
     * specific class.
*/
static String[] expandTypeParam(String[] types, JsonLd jsonld) {
        // Filter out all types that have (more specific) subclasses that are
        // also in the list.
        // So for example [Instance, Electronic] should be reduced to just
        // [Electronic].
        // Afterwards, include all subclasses of the remaining types.
        Set<String> subClasses = new HashSet<>();

        // Select types to prune
        Set<String> toBeRemoved = new HashSet<>();
        for (String c1 : types) {
            ArrayList<String> c1SuperClasses = new ArrayList<String>();
            jsonld.getSuperClasses(c1, c1SuperClasses);
            toBeRemoved.addAll(c1SuperClasses);
        }
        // Make a new pruned list without the undesired superclasses
        List<String> prunedTypes = new ArrayList<String>();
        for (String type : types) {
            if (!toBeRemoved.contains(type))
                prunedTypes.add(type);
        }
        // Add all subclasses of the remaining types
        for (String type : prunedTypes) {
            //subClasses += jsonld.getSubClasses(type);
            subClasses.addAll(jsonld.getSubClasses(type));
            subClasses.add(type);
        }

        //return subClasses.toArray();
        return subClasses.toArray(new String[0]);
}

}
