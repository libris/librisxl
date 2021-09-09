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

import static trld.Common.loadJson;
import static trld.Common.warning;
import static trld.Common.resolveIri;
import static trld.jsonld.Base.*;
import static trld.jsonld.Context.*;


public class Term { // LINE: 454
  public String iri; // LINE: 456
  public Boolean isPrefix; // LINE: 457
  public Boolean isProtected; // LINE: 458
  public Boolean isReverseProperty; // LINE: 459
  public /*@Nullable*/ String baseUrl; // LINE: 460
  public Boolean hasLocalContext; // LINE: 461
  public List<String> container; // LINE: 462
  public /*@Nullable*/ String direction; // LINE: 463
  public /*@Nullable*/ String index; // LINE: 464
  public /*@Nullable*/ String language; // LINE: 465
  public /*@Nullable*/ String nestValue; // LINE: 466
  public /*@Nullable*/ String typeMapping; // LINE: 467
  public /*@Nullable*/ Object localContext; // LINE: 469
  public Map<String, /*@Nullable*/ Context> cachedContexts; // LINE: 470
  public Set remoteContexts; // LINE: 471

  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined) {
    this(activeContext, localContext, term, value, defined, null);
  }
  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined, String baseUrl) {
    this(activeContext, localContext, term, value, defined, baseUrl, false);
  }
  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined, String baseUrl, Boolean isprotected) {
    this(activeContext, localContext, term, value, defined, baseUrl, isprotected, false);
  }
  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined, String baseUrl, Boolean isprotected, Boolean overrideProtected) {
    this(activeContext, localContext, term, value, defined, baseUrl, isprotected, overrideProtected, null);
  }
  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined, String baseUrl, Boolean isprotected, Boolean overrideProtected, Set remoteContexts) {
    this(activeContext, localContext, term, value, defined, baseUrl, isprotected, overrideProtected, remoteContexts, true);
  }
  public Term(Context activeContext, Map<String, Object> localContext, String term, Object value, Map<String, Boolean> defined, String baseUrl, Boolean isprotected, Boolean overrideProtected, Set remoteContexts, Boolean validateScoped) { // LINE: 473
    this.isPrefix = false; // LINE: 486
    this.isProtected = (isprotected instanceof Boolean ? (Boolean) isprotected : false); // LINE: 487
    this.isReverseProperty = false; // LINE: 488
    this.container = new ArrayList<>(); // LINE: 489
    this.direction = null; // LINE: 490
    this.index = null; // LINE: 491
    this.language = null; // LINE: 492
    this.nestValue = null; // LINE: 493
    this.typeMapping = null; // LINE: 494
    if (remoteContexts == null) { // LINE: 496
      remoteContexts = new HashSet(); // LINE: 497
    }
    this.baseUrl = null; // LINE: 499
    this.hasLocalContext = false; // LINE: 500
    this.localContext = null; // LINE: 501
    this.remoteContexts = remoteContexts; // LINE: 502
    this.cachedContexts = new HashMap<>(); // LINE: 503
    if (defined.containsKey(term)) { // LINE: 507
      Boolean definedTerm = (Boolean) defined.get(term); // LINE: 508
      if (definedTerm) { // LINE: 509
        return; // LINE: 510
      } else {
        throw new CyclicIriMappingError(term); // LINE: 512
      }
    }
    if ((term == null && ((Object) "") == null || term != null && (term).equals(""))) { // LINE: 514
      throw new InvalidTermDefinitionError(); // LINE: 515
    }
    defined.put(term, false); // LINE: 517
    if ((term == null && ((Object) TYPE) == null || term != null && (term).equals(TYPE))) { // LINE: 523
      if ((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10))) { // LINE: 524
        throw new KeywordRedefinitionError(); // LINE: 525
      }
      if ((!(value instanceof Map) || !(((((Map) value).get(CONTAINER) == null && ((Object) SET) == null || ((Map) value).get(CONTAINER) != null && (((Map) value).get(CONTAINER)).equals(SET)) || ((Map) value).containsKey(PROTECTED))))) { // LINE: 527
        throw new KeywordRedefinitionError(); // LINE: 529
      }
    } else if (KEYWORDS.contains(term)) { // LINE: 532
      throw new KeywordRedefinitionError(term); // LINE: 533
    }
    if (hasKeywordForm(term)) { // LINE: 535
      warning("Term " + term + " looks like a keyword (it matches the ABNF rule \"@\"1*ALPHA from [RFC5234])"); // LINE: 536
    }
    /*@Nullable*/ Term prevDfn = (/*@Nullable*/ Term) activeContext.terms.remove(term); // LINE: 540
    Boolean simpleTerm; // LINE: 542
    Map dfn; // LINE: 545
    if ((value == null || value instanceof String)) { // LINE: 546
      dfn = new HashMap<>(); // LINE: 547
      dfn.put(ID, (String) value); // LINE: 548
      simpleTerm = value instanceof String; // LINE: 549
    } else {
      if (!(value instanceof Map)) { // LINE: 552
        throw new InvalidTermDefinitionError(value.toString()); // LINE: 553
      }
      dfn = (Map) value; // LINE: 554
      simpleTerm = false; // LINE: 555
    }
    if (dfn.containsKey(PROTECTED)) { // LINE: 561
      if ((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10))) { // LINE: 562
        throw new InvalidTermDefinitionError(dfn.toString()); // LINE: 563
      }
      Object isProtected = (Object) dfn.get(PROTECTED); // LINE: 564
      if (isProtected instanceof Boolean) { // LINE: 565
        this.isProtected = (Boolean) isProtected; // LINE: 566
      } else {
        throw new InvalidProtectedValueError(); // LINE: 568
      }
    }
    Object typeMapping = (Object) dfn.get(TYPE); // LINE: 571
    if (typeMapping instanceof String) { // LINE: 573
      this.typeMapping = (String) activeContext.expandInitVocabIri((String) typeMapping, localContext, defined); // LINE: 575
      if (((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10)) && new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) JSON, NONE}))).contains(this.typeMapping))) { // LINE: 577
        throw new InvalidTypeMappingError(); // LINE: 579
      } else if ((!new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) ID, JSON, NONE, VOCAB}))).contains(this.typeMapping) && !(isIri(this.typeMapping)))) { // LINE: 581
        throw new InvalidTypeMappingError(this.typeMapping); // LINE: 582
      }
    } else if (typeMapping != null) { // LINE: 583
      throw new InvalidTypeMappingError(); // LINE: 584
    }
    if (dfn.containsKey(REVERSE)) { // LINE: 587
      Object rev = (Object) dfn.get(REVERSE); // LINE: 588
      if ((dfn.containsKey(ID) || dfn.containsKey(NEST))) { // LINE: 590
        throw new InvalidReversePropertyError(); // LINE: 591
      }
      if (!(rev instanceof String)) { // LINE: 593
        throw new InvalidIriMappingError(); // LINE: 594
      }
      if (hasKeywordForm((String) rev)) { // LINE: 597
        warning("Reverse " + rev + " for term " + term + " has the form of a keyword"); // LINE: 598
        return; // LINE: 599
      }
      this.iri = ((String) activeContext.expandInitVocabIri((String) rev, localContext, defined)); // LINE: 601
      if (!((isBlank(this.iri) || isIri(this.iri)))) { // LINE: 602
        throw new InvalidIriMappingError(); // LINE: 603
      }
      if (dfn.containsKey(CONTAINER)) { // LINE: 605
        this.container = (List<String>) Builtins.sorted(asList(dfn.get(CONTAINER))); // LINE: 606
        if (!new HashSet(new ArrayList<>(Arrays.asList(new Object[] {(Object) SET, INDEX, null}))).contains(dfn.get(CONTAINER))) { // LINE: 607
          throw new InvalidReversePropertyError(); // LINE: 608
        }
      }
      this.isReverseProperty = true; // LINE: 610
      activeContext.terms.put(term, this); // LINE: 612
      defined.put(term, true); // LINE: 613
      return; // LINE: 614
    }
    if ((dfn.containsKey(ID) && !term.equals(dfn.get(ID)))) { // LINE: 617
      String id = (String) ((String) dfn.get(ID)); // LINE: 618
      if (id == null) { // LINE: 620
        this.iri = null; // LINE: 622
      } else {
        if (!(id instanceof String)) { // LINE: 626
          throw new InvalidIriMappingError(); // LINE: 627
        }
        if ((!KEYWORDS.contains(id) && hasKeywordForm((String) id))) { // LINE: 629
          warning("Id " + id + " for term " + term + " is not a keyword but has the form of a keyword"); // LINE: 630
        }
        this.iri = ((String) activeContext.expandInitVocabIri((String) id, localContext, defined)); // LINE: 633
        if ((this.iri == null || !((KEYWORDS.contains(this.iri) || isBlank(this.iri) || isIri(this.iri))))) { // LINE: 634
          return; // LINE: 636
        }
        if ((this.iri == null && ((Object) CONTEXT) == null || this.iri != null && (this.iri).equals(CONTEXT))) { // LINE: 638
          throw new InvalidKeywordAliasError(); // LINE: 639
        }
        if ((term.substring(1, term.length() - 1).contains(":") || term.contains("/"))) { // LINE: 642
          defined.put(term, true); // LINE: 644
          if (!activeContext.expandInitVocabIri(term, localContext, defined).equals(this.iri)) { // LINE: 646
          }
        } else if ((simpleTerm && (PREFIX_DELIMS.contains(this.iri.substring(this.iri.length() - 1, this.iri.length() - 1 + 1)) || isBlank(this.iri)))) { // LINE: 652
          this.isPrefix = true; // LINE: 653
        }
      }
    } else if (term.substring(1).contains(":")) { // LINE: 656
      Integer idx = (Integer) term.indexOf(":"); // LINE: 658
      String prefix = term.substring(0, idx); // LINE: 659
      String suffix = term.substring(idx + 1); // LINE: 660
      if (localContext.containsKey(prefix)) { // LINE: 663
        new Term(activeContext, localContext, prefix, localContext.get(prefix), defined); // LINE: 664
      }
      if (activeContext.terms.containsKey(prefix)) { // LINE: 667
        this.iri = activeContext.terms.get(prefix).iri + suffix; // LINE: 668
      } else {
        this.iri = term; // LINE: 672
      }
    } else if (term.contains("/")) { // LINE: 675
      this.iri = ((String) activeContext.expandVocabIri(term)); // LINE: 678
      if (!(isIri(this.iri))) { // LINE: 679
        throw new InvalidIriMappingError(); // LINE: 680
      }
    } else if ((term == null && ((Object) TYPE) == null || term != null && (term).equals(TYPE))) { // LINE: 683
      this.iri = TYPE; // LINE: 684
    } else if (activeContext.vocabularyMapping != null) { // LINE: 687
      this.iri = activeContext.vocabularyMapping + term; // LINE: 688
    } else {
      throw new InvalidIriMappingError(term + ": " + value.toString()); // LINE: 690
    }
    if (dfn.containsKey(CONTAINER)) { // LINE: 693
      Object container = (Object) dfn.get(CONTAINER); // LINE: 694
      /*@Nullable*/ Set containerTerms = null; // LINE: 696
      if (container instanceof List) { // LINE: 697
        containerTerms = (Set) new HashSet((List) container); // LINE: 698
        if ((containerTerms.contains(SET) && !containerTerms.contains(LIST))) { // LINE: 699
          containerTerms.remove(SET); // LINE: 700
        }
      }
      if ((!((container instanceof String && CONTAINER_KEYWORDS.contains(container))) && !((containerTerms != null && (containerTerms.size() == 0 || (containerTerms.size() == 1 && containerTerms.stream().allMatch(t -> CONTAINER_KEYWORDS.contains(t))) || (containerTerms == null && ((Object) new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) GRAPH, ID})))) == null || containerTerms != null && (containerTerms).equals(new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) GRAPH, ID}))))) || (containerTerms == null && ((Object) new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) GRAPH, INDEX})))) == null || containerTerms != null && (containerTerms).equals(new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) GRAPH, INDEX}))))) || new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) INDEX, GRAPH, ID, TYPE, LANGUAGE}))).contains(new ArrayList(containerTerms).get(0))))))) { // LINE: 701
        throw new InvalidContainerMappingError((String) container); // LINE: 712
      }
      if ((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10))) { // LINE: 715
        if ((!(container instanceof String) || new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) GRAPH, ID, TYPE}))).contains(container))) { // LINE: 716
          throw new InvalidContainerMappingError((String) container.toString()); // LINE: 717
        }
      }
      this.container = (List<String>) Builtins.sorted(asList(((Object) container))); // LINE: 720
      if (this.container.contains(TYPE)) { // LINE: 723
        if (this.typeMapping == null) { // LINE: 725
          this.typeMapping = ID; // LINE: 726
        } else if (!new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) ID, VOCAB}))).contains(this.typeMapping)) { // LINE: 728
          throw new InvalidTypeMappingError(); // LINE: 729
        }
      }
    }
    if (dfn.containsKey(INDEX)) { // LINE: 732
      if (((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10)) || !this.container.contains(INDEX))) { // LINE: 734
        throw new InvalidTermDefinitionError(value.toString()); // LINE: 735
      }
      Object index = (Object) dfn.get(INDEX); // LINE: 737
      if (!(index instanceof String)) { // LINE: 738
        throw new InvalidTermDefinitionError(value.toString()); // LINE: 739
      }
      if (!(isIri(activeContext.expandVocabIri((String) index)))) { // LINE: 740
        throw new InvalidTermDefinitionError(value.toString()); // LINE: 741
      }
      this.index = (String) index; // LINE: 743
    }
    this.hasLocalContext = (Boolean) dfn.containsKey(CONTEXT); // LINE: 746
    if (this.hasLocalContext) { // LINE: 747
      if ((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10))) { // LINE: 749
        throw new InvalidTermDefinitionError(dfn.toString()); // LINE: 750
      }
      this.localContext = (Object) dfn.get(CONTEXT); // LINE: 761
      this.baseUrl = baseUrl; // LINE: 762
    }
    if ((dfn.containsKey(LANGUAGE) && !dfn.containsKey(TYPE))) { // LINE: 765
      Object lang = (Object) dfn.get(LANGUAGE); // LINE: 767
      if ((!(lang instanceof String) && lang != null)) { // LINE: 768
        throw new InvalidLanguageMappingError(); // LINE: 769
      }
      if (!(isLangTag((String) lang))) { // LINE: 770
        warning("Language tag " + lang + " in term " + term + " is not well-formed"); // LINE: 771
      }
      this.language = (lang == null ? NULL : ((String) lang).toLowerCase()); // LINE: 774
    }
    if ((dfn.containsKey(DIRECTION) && !dfn.containsKey(TYPE))) { // LINE: 777
      Object dir = (Object) dfn.get(DIRECTION); // LINE: 779
      if ((!DIRECTIONS.contains(dir) && dir != null)) { // LINE: 780
        throw new InvalidBaseDirectionError(); // LINE: 781
      }
      this.direction = (dir == null ? NULL : ((String) dir)); // LINE: 784
    }
    if (dfn.containsKey(NEST)) { // LINE: 787
      if ((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10))) { // LINE: 789
        throw new InvalidTermDefinitionError(); // LINE: 790
      }
      this.nestValue = ((String) dfn.get(NEST)); // LINE: 792
      if ((!(this.nestValue instanceof String) || (!this.nestValue.equals(NEST) && KEYWORDS.contains(this.nestValue)))) { // LINE: 793
        throw new InvalidNestValueError(); // LINE: 795
      }
    }
    if (dfn.containsKey(PREFIX)) { // LINE: 798
      if (((activeContext.processingMode == null && ((Object) JSONLD10) == null || activeContext.processingMode != null && (activeContext.processingMode).equals(JSONLD10)) || term.contains(":") || term.contains("/"))) { // LINE: 800
        throw new InvalidTermDefinitionError(); // LINE: 801
      }
      this.isPrefix = ((Boolean) dfn.get(PREFIX)); // LINE: 803
      if (!(this.isPrefix instanceof Boolean)) { // LINE: 804
        throw new InvalidPrefixValueError(); // LINE: 805
      }
      if ((this.isPrefix && KEYWORDS.contains(this.iri))) { // LINE: 807
        throw new InvalidTermDefinitionError(); // LINE: 808
      }
    }
    if ((!(overrideProtected) && prevDfn != null && prevDfn.isProtected)) { // LINE: 815
      this.isProtected = (Boolean) prevDfn.isProtected; // LINE: 817
      if (!(this.matches(prevDfn))) { // LINE: 819
        throw new ProtectedTermRedefinitionError(); // LINE: 820
      }
    }
    activeContext.terms.put(term, this); // LINE: 823
    defined.put(term, true); // LINE: 824
  }

  public Context getLocalContext(Context activeContext) {
    return this.getLocalContext(activeContext, true);
  }
  public Context getLocalContext(Context activeContext, Boolean propagate) { // LINE: 826
    String cacheKey = activeContext.hashCode() + ":" + propagate.toString(); // LINE: 827
    /*@Nullable*/ Context cached = (/*@Nullable*/ Context) this.cachedContexts.get(cacheKey); // LINE: 828
    Boolean overrideProtected = propagate; // LINE: 832
    if (cached == null) { // LINE: 834
      cached = activeContext.getContext(this.localContext, this.baseUrl, new HashSet(this.remoteContexts), overrideProtected, false); // LINE: 835
      this.cachedContexts.put(cacheKey, cached); // LINE: 840
    }
    if ((!(this.localContext instanceof Map) || !((Map) this.localContext).containsKey(PROPAGATE))) { // LINE: 842
      cached.propagate = propagate; // LINE: 844
    }
    return cached; // LINE: 846
  }

  public boolean matches(Object other) { // LINE: 848
    if (!(other instanceof Term)) { // LINE: 849
      return false; // LINE: 850
    }
    return ((this.iri == null && ((Object) ((Term) other).iri) == null || this.iri != null && (this.iri).equals(((Term) other).iri)) && (this.isPrefix == null && ((Object) ((Term) other).isPrefix) == null || this.isPrefix != null && (this.isPrefix).equals(((Term) other).isPrefix)) && (this.isReverseProperty == null && ((Object) ((Term) other).isReverseProperty) == null || this.isReverseProperty != null && (this.isReverseProperty).equals(((Term) other).isReverseProperty)) && (this.baseUrl == null && ((Object) ((Term) other).baseUrl) == null || this.baseUrl != null && (this.baseUrl).equals(((Term) other).baseUrl)) && (this.hasLocalContext == null && ((Object) ((Term) other).hasLocalContext) == null || this.hasLocalContext != null && (this.hasLocalContext).equals(((Term) other).hasLocalContext)) && (this.container == null && ((Object) ((Term) other).container) == null || this.container != null && (this.container).equals(((Term) other).container)) && (this.direction == null && ((Object) ((Term) other).direction) == null || this.direction != null && (this.direction).equals(((Term) other).direction)) && (this.index == null && ((Object) ((Term) other).index) == null || this.index != null && (this.index).equals(((Term) other).index)) && (this.language == null && ((Object) ((Term) other).language) == null || this.language != null && (this.language).equals(((Term) other).language)) && (this.nestValue == null && ((Object) ((Term) other).nestValue) == null || this.nestValue != null && (this.nestValue).equals(((Term) other).nestValue)) && (this.typeMapping == null && ((Object) ((Term) other).typeMapping) == null || this.typeMapping != null && (this.typeMapping).equals(((Term) other).typeMapping)) && (this.localContext == null && ((Object) ((Term) other).localContext) == null || this.localContext != null && (this.localContext).equals(((Term) other).localContext))); // LINE: 852
  }
}