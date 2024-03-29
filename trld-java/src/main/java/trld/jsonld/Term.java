/**
 * This file was automatically generated by the TRLD transpiler.
 * Source: trld/jsonld/context.py
 */
package trld.jsonld;

//import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.io.*;

import trld.Builtins;
import trld.KeyValue;

import trld.jsonld.LoadDocumentCallback;
import trld.jsonld.LoadDocumentOptions;
import static trld.jsonld.Docloader.getDocumentLoader;
import static trld.platform.Common.resolveIri;
import static trld.platform.Common.warning;
import static trld.jsonld.Base.*;
import static trld.jsonld.Context.*;


public class Term {
  public String iri;
  public Boolean isPrefix;
  public Boolean isProtected;
  public Boolean isReverseProperty;
  public /*@Nullable*/ String baseUrl;
  public Boolean hasLocalContext;
  public List<String> container;
  public /*@Nullable*/ String direction;
  public /*@Nullable*/ String index;
  public /*@Nullable*/ String language;
  public /*@Nullable*/ String nestValue;
  public /*@Nullable*/ String typeMapping;
  public /*@Nullable*/ Object localContext;
  public Map<String, /*@Nullable*/ Context> cachedContexts;
  public Set remoteContexts;

  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined) {
    this(activeContext, localContext, term, value, defined, null);
  }
  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined, /*@Nullable*/ String baseUrl) {
    this(activeContext, localContext, term, value, defined, baseUrl, false);
  }
  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined, /*@Nullable*/ String baseUrl, Boolean isprotected) {
    this(activeContext, localContext, term, value, defined, baseUrl, isprotected, false);
  }
  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined, /*@Nullable*/ String baseUrl, Boolean isprotected, Boolean overrideProtected) {
    this(activeContext, localContext, term, value, defined, baseUrl, isprotected, overrideProtected, null);
  }
  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined, /*@Nullable*/ String baseUrl, Boolean isprotected, Boolean overrideProtected, /*@Nullable*/ Set remoteContexts) {
    this(activeContext, localContext, term, value, defined, baseUrl, isprotected, overrideProtected, remoteContexts, true);
  }
  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined, /*@Nullable*/ String baseUrl, Boolean isprotected, Boolean overrideProtected, /*@Nullable*/ Set remoteContexts, Boolean validateScoped) {
    this.isPrefix = false;
    this.isProtected = (isprotected instanceof Boolean ? (Boolean) isprotected : false);
    this.isReverseProperty = false;
    this.container = new ArrayList<>();
    this.direction = null;
    this.index = null;
    this.language = null;
    this.nestValue = null;
    this.typeMapping = null;
    if (remoteContexts == null) {
      remoteContexts = new HashSet();
    }
    this.baseUrl = null;
    this.hasLocalContext = false;
    this.localContext = null;
    this.remoteContexts = remoteContexts;
    this.cachedContexts = new HashMap<>();
    if (defined.containsKey(term)) {
      Boolean definedTerm = (Boolean) defined.get(term);
      if (definedTerm) {
        return;
      } else {
        throw new CyclicIriMappingError(term);
      }
    }
    if ((term == null && ((Object) "") == null || term != null && (term).equals(""))) {
      throw new InvalidTermDefinitionError();
    }
    defined.put(term, false);
    if ((term == null && ((Object) TYPE) == null || term != null && (term).equals(TYPE))) {
      if ((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10))) {
        throw new KeywordRedefinitionError();
      }
      if ((!(value instanceof Map) || !(((((Map) value).get(CONTAINER) == null && ((Object) SET) == null || ((Map) value).get(CONTAINER) != null && (((Map) value).get(CONTAINER)).equals(SET)) || ((Map) value).containsKey(PROTECTED))))) {
        throw new KeywordRedefinitionError();
      }
    } else if (KEYWORDS.contains(term)) {
      throw new KeywordRedefinitionError(term);
    }
    if (hasKeywordForm(term)) {
      warning("Term " + term + " looks like a keyword (it matches the ABNF rule \"@\"1*ALPHA from [RFC5234])");
    }
    /*@Nullable*/ Term prevDfn = (/*@Nullable*/ Term) activeContext.terms.remove(term);
    Boolean simpleTerm;
    Map dfn;
    if ((value == null || value instanceof String)) {
      dfn = new HashMap<>();
      dfn.put(ID, (String) value);
      simpleTerm = value instanceof String;
    } else {
      if (!(value instanceof Map)) {
        throw new InvalidTermDefinitionError(value.toString());
      }
      dfn = (Map) value;
      simpleTerm = false;
    }
    if (dfn.containsKey(PROTECTED)) {
      if ((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10))) {
        throw new InvalidTermDefinitionError(dfn.toString());
      }
      Object isProtected = (Object) dfn.get(PROTECTED);
      if (isProtected instanceof Boolean) {
        this.isProtected = (Boolean) isProtected;
      } else {
        throw new InvalidProtectedValueError();
      }
    }
    Object typeMapping = (Object) dfn.get(TYPE);
    if (typeMapping instanceof String) {
      this.typeMapping = (String) activeContext.expandInitVocabIri((String) typeMapping, localContext, defined);
      if (((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10)) && new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) JSON, NONE}))).contains(this.typeMapping))) {
        throw new InvalidTypeMappingError();
      } else if ((!new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) ID, JSON, NONE, VOCAB}))).contains(this.typeMapping) && !(isIri(this.typeMapping)))) {
        throw new InvalidTypeMappingError(this.typeMapping);
      }
    } else if (typeMapping != null) {
      throw new InvalidTypeMappingError();
    }
    if (dfn.containsKey(REVERSE)) {
      Object rev = (Object) dfn.get(REVERSE);
      if ((dfn.containsKey(ID) || dfn.containsKey(NEST))) {
        throw new InvalidReversePropertyError();
      }
      if (!(rev instanceof String)) {
        throw new InvalidIriMappingError();
      }
      if (hasKeywordForm((String) rev)) {
        warning("Reverse " + rev + " for term " + term + " has the form of a keyword");
        return;
      }
      this.iri = ((String) activeContext.expandInitVocabIri((String) rev, localContext, defined));
      if (!((isBlank(this.iri) || isIri(this.iri)))) {
        throw new InvalidIriMappingError();
      }
      if (dfn.containsKey(CONTAINER)) {
        this.container = (List<String>) Builtins.sorted(asList(dfn.get(CONTAINER)));
        if (!new HashSet(new ArrayList<>(Arrays.asList(new Object[] {(Object) SET, INDEX, null}))).contains(dfn.get(CONTAINER))) {
          throw new InvalidReversePropertyError();
        }
      }
      this.isReverseProperty = true;
      activeContext.terms.put(term, this);
      defined.put(term, true);
      return;
    }
    if ((dfn.containsKey(ID) && !term.equals(dfn.get(ID)))) {
      String id = (String) ((String) dfn.get(ID));
      if (id == null) {
        this.iri = null;
      } else {
        if (!(id instanceof String)) {
          throw new InvalidIriMappingError();
        }
        if ((!KEYWORDS.contains(id) && hasKeywordForm((String) id))) {
          warning("Id " + id + " for term " + term + " is not a keyword but has the form of a keyword");
        }
        this.iri = ((String) activeContext.expandInitVocabIri((String) id, localContext, defined));
        if ((this.iri == null || !((KEYWORDS.contains(this.iri) || isBlank(this.iri) || isIri(this.iri))))) {
          return;
        }
        if ((this.iri == null && ((Object) CONTEXT) == null || this.iri != null && (this.iri).equals(CONTEXT))) {
          throw new InvalidKeywordAliasError();
        }
        if (((term.length() >= 1 ? term.substring(1, term.length() - 1) : "").contains(":") || term.contains("/"))) {
          defined.put(term, true);
          if (!activeContext.expandInitVocabIri(term, localContext, defined).equals(this.iri)) {
          }
        } else if ((simpleTerm && (PREFIX_DELIMS.contains(this.iri.substring(this.iri.length() - 1, this.iri.length() - 1 + 1)) || isBlank(this.iri)))) {
          this.isPrefix = true;
        }
      }
    } else if ((term.length() >= 1 ? term.substring(1) : "").contains(":")) {
      Integer idx = (Integer) term.indexOf(":");
      String prefix = (term.length() >= 0 ? term.substring(0, idx) : "");
      String suffix = (term.length() >= idx + 1 ? term.substring(idx + 1) : "");
      if (localContext.containsKey(prefix)) {
        new Term(activeContext, localContext, prefix, localContext.get(prefix), defined);
      }
      if (activeContext.terms.containsKey(prefix)) {
        this.iri = activeContext.terms.get(prefix).iri + suffix;
      } else {
        this.iri = term;
      }
    } else if (term.contains("/")) {
      this.iri = ((String) activeContext.expandVocabIri(term));
      if (!(isIri(this.iri))) {
        throw new InvalidIriMappingError();
      }
    } else if ((term == null && ((Object) TYPE) == null || term != null && (term).equals(TYPE))) {
      this.iri = TYPE;
    } else if (activeContext.vocabularyMapping != null) {
      this.iri = activeContext.vocabularyMapping + term;
    } else {
      throw new InvalidIriMappingError(term + ": " + value.toString());
    }
    if (dfn.containsKey(CONTAINER)) {
      Object container = (Object) dfn.get(CONTAINER);
      /*@Nullable*/ Set containerTerms = null;
      if (container instanceof List) {
        containerTerms = (Set) new HashSet((List) container);
        if ((containerTerms.contains(SET) && !containerTerms.contains(LIST))) {
          containerTerms.remove(SET);
        }
      }
      if ((!((container instanceof String && CONTAINER_KEYWORDS.contains(container))) && !((containerTerms != null && (containerTerms.size() == 0 || (containerTerms.size() == 1 && containerTerms.stream().allMatch(t -> CONTAINER_KEYWORDS.contains(t))) || (containerTerms == null && ((Object) new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) GRAPH, ID})))) == null || containerTerms != null && (containerTerms).equals(new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) GRAPH, ID}))))) || (containerTerms == null && ((Object) new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) GRAPH, INDEX})))) == null || containerTerms != null && (containerTerms).equals(new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) GRAPH, INDEX}))))) || new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) INDEX, GRAPH, ID, TYPE, LANGUAGE}))).contains(new ArrayList(containerTerms).get(0))))))) {
        throw new InvalidContainerMappingError((String) container);
      }
      if ((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10))) {
        if ((!(container instanceof String) || new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) GRAPH, ID, TYPE}))).contains(container))) {
          throw new InvalidContainerMappingError((String) container.toString());
        }
      }
      this.container = (List<String>) Builtins.sorted(asList(((Object) container)));
      if (this.container.contains(TYPE)) {
        if (this.typeMapping == null) {
          this.typeMapping = ID;
        } else if (!new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) ID, VOCAB}))).contains(this.typeMapping)) {
          throw new InvalidTypeMappingError();
        }
      }
    }
    if (dfn.containsKey(INDEX)) {
      if (((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10)) || !this.container.contains(INDEX))) {
        throw new InvalidTermDefinitionError(value.toString());
      }
      Object index = (Object) dfn.get(INDEX);
      if (!(index instanceof String)) {
        throw new InvalidTermDefinitionError(value.toString());
      }
      if (!(isIri(activeContext.expandVocabIri((String) index)))) {
        throw new InvalidTermDefinitionError(value.toString());
      }
      this.index = (String) index;
    }
    this.hasLocalContext = (Boolean) dfn.containsKey(CONTEXT);
    if (this.hasLocalContext) {
      if ((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10))) {
        throw new InvalidTermDefinitionError(dfn.toString());
      }
      this.localContext = (Object) dfn.get(CONTEXT);
      this.baseUrl = baseUrl;
    }
    if ((dfn.containsKey(LANGUAGE) && !dfn.containsKey(TYPE))) {
      Object lang = (Object) dfn.get(LANGUAGE);
      if ((!(lang instanceof String) && lang != null)) {
        throw new InvalidLanguageMappingError();
      }
      if (!(isLangTag((String) lang))) {
        warning("Language tag " + lang + " in term " + term + " is not well-formed");
      }
      this.language = (lang == null ? NULL : ((String) lang).toLowerCase());
    }
    if ((dfn.containsKey(DIRECTION) && !dfn.containsKey(TYPE))) {
      Object dir = (Object) dfn.get(DIRECTION);
      if ((!DIRECTIONS.contains(dir) && dir != null)) {
        throw new InvalidBaseDirectionError();
      }
      this.direction = (dir == null ? NULL : ((String) dir));
    }
    if (dfn.containsKey(NEST)) {
      if ((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10))) {
        throw new InvalidTermDefinitionError();
      }
      this.nestValue = ((String) dfn.get(NEST));
      if ((!(this.nestValue instanceof String) || (!this.nestValue.equals(NEST) && KEYWORDS.contains(this.nestValue)))) {
        throw new InvalidNestValueError();
      }
    }
    if (dfn.containsKey(PREFIX)) {
      if (((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10)) || term.contains(":") || term.contains("/"))) {
        throw new InvalidTermDefinitionError();
      }
      this.isPrefix = ((Boolean) dfn.get(PREFIX));
      if (!(this.isPrefix instanceof Boolean)) {
        throw new InvalidPrefixValueError();
      }
      if ((this.isPrefix && KEYWORDS.contains(this.iri))) {
        throw new InvalidTermDefinitionError();
      }
    }
    if ((!(overrideProtected) && prevDfn != null && prevDfn.isProtected)) {
      this.isProtected = (Boolean) prevDfn.isProtected;
      if (!(this.matches(prevDfn))) {
        throw new ProtectedTermRedefinitionError();
      }
    }
    activeContext.terms.put(term, this);
    defined.put(term, true);
  }

  public Context getLocalContext(Context activeContext) {
    return this.getLocalContext(activeContext, true);
  }
  public Context getLocalContext(Context activeContext, Boolean propagate) {
    String cacheKey = activeContext.hashCode() + ":" + propagate.toString();
    /*@Nullable*/ Context cached = (/*@Nullable*/ Context) this.cachedContexts.get(cacheKey);
    Boolean overrideProtected = propagate;
    if (cached == null) {
      cached = activeContext.getContext(this.localContext, this.baseUrl, new HashSet(this.remoteContexts), overrideProtected, false);
      this.cachedContexts.put(cacheKey, cached);
    }
    if ((!(this.localContext instanceof Map) || !((Map) this.localContext).containsKey(PROPAGATE))) {
      cached.propagate = propagate;
    }
    return cached;
  }

  public boolean matches(Object other) {
    if (!(other instanceof Term)) {
      return false;
    }
    return ((this.iri == null && ((Object) ((Term) other).iri) == null || this.iri != null && (this.iri).equals(((Term) other).iri)) && (this.isPrefix == null && ((Object) ((Term) other).isPrefix) == null || this.isPrefix != null && (this.isPrefix).equals(((Term) other).isPrefix)) && (this.isReverseProperty == null && ((Object) ((Term) other).isReverseProperty) == null || this.isReverseProperty != null && (this.isReverseProperty).equals(((Term) other).isReverseProperty)) && (this.baseUrl == null && ((Object) ((Term) other).baseUrl) == null || this.baseUrl != null && (this.baseUrl).equals(((Term) other).baseUrl)) && (this.hasLocalContext == null && ((Object) ((Term) other).hasLocalContext) == null || this.hasLocalContext != null && (this.hasLocalContext).equals(((Term) other).hasLocalContext)) && (this.container == null && ((Object) ((Term) other).container) == null || this.container != null && (this.container).equals(((Term) other).container)) && (this.direction == null && ((Object) ((Term) other).direction) == null || this.direction != null && (this.direction).equals(((Term) other).direction)) && (this.index == null && ((Object) ((Term) other).index) == null || this.index != null && (this.index).equals(((Term) other).index)) && (this.language == null && ((Object) ((Term) other).language) == null || this.language != null && (this.language).equals(((Term) other).language)) && (this.nestValue == null && ((Object) ((Term) other).nestValue) == null || this.nestValue != null && (this.nestValue).equals(((Term) other).nestValue)) && (this.typeMapping == null && ((Object) ((Term) other).typeMapping) == null || this.typeMapping != null && (this.typeMapping).equals(((Term) other).typeMapping)) && (this.localContext == null && ((Object) ((Term) other).localContext) == null || this.localContext != null && (this.localContext).equals(((Term) other).localContext)));
  }
}
