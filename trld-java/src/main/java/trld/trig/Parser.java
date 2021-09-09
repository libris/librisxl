/**
 * This file was automatically generated by the TRLD transpiler.
 * Source: trld/trig/parser.py
 */
package trld.trig;

//import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.io.*;

import trld.Builtins;
import trld.KeyValue;

import trld.Input;
import static trld.Common.dumpJson;
import static trld.jsonld.Base.VALUE;
import static trld.jsonld.Base.TYPE;
import static trld.jsonld.Base.LANGUAGE;
import static trld.jsonld.Base.ID;
import static trld.jsonld.Base.LIST;
import static trld.jsonld.Base.GRAPH;
import static trld.jsonld.Base.CONTEXT;
import static trld.jsonld.Base.VOCAB;
import static trld.jsonld.Base.BASE;
import static trld.jsonld.Base.PREFIX;
import static trld.jsonld.Base.PREFIX_DELIMS;
import static trld.Rdfterms.RDF_TYPE;
import static trld.Rdfterms.XSD;
import static trld.Rdfterms.XSD_DOUBLE;
import static trld.Rdfterms.XSD_INTEGER;

public class Parser {
  public static final String ANNOTATION = "@annotation"; // LINE: 13
  public static final String XSD_DECIMAL = XSD + "decimal"; // LINE: 15
  public static final Set<String> AT_KEYWORDS = new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) PREFIX, BASE}))); // LINE: 17
  public static final String RQ_PREFIX = "prefix"; // LINE: 19
  public static final String RQ_BASE = "base"; // LINE: 20
  public static final String RQ_GRAPH = "graph"; // LINE: 21
  public static final Set<String> RQ_KEYWORDS = new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) RQ_PREFIX, RQ_BASE, RQ_GRAPH}))); // LINE: 23
  public static final Map<String, String> ESC_CHARS = Builtins.mapOf("t", "\t", "b", "", "n", "\n", "r", "\r", "f", "\u000c", "\"", "\"", "'", "'", "\\", "\\"); // LINE: 25
  public static final Set<String> RESERVED_CHARS = new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) "~", ".", "-", "!", "$", "&", "'", "(", ")", "*", "+", ",", ";", "=", "/", "?", "#", "@", "%", "_"}))); // LINE: 36
  public static final Pattern NUMBER_LEAD_CHARS = (Pattern) Pattern.compile("[+-.0-9]"); // LINE: 42
  public static final Pattern TURTLE_INT_CHARS = (Pattern) Pattern.compile("[+-.0-9]"); // LINE: 44
  public static final Set<String> LITERAL_QUOTE_CHARS = new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) "\"", "'"}))); // LINE: 46
  public static final String SYMBOL = "@symbol"; // LINE: 48
  public static final String EOF = ""; // LINE: 49

  public static Object parse(Input inp) { // LINE: 801
    ParserState state = new ReadNodes(null); // LINE: 802
    Object value = null; // LINE: 803
    Integer lno = 1; // LINE: 805
    Integer cno = 1; // LINE: 806
    for (String c : ((Iterable<String>) inp.characters())) { // LINE: 807
      if ((c == null && ((Object) "\n") == null || c != null && (c).equals("\n"))) { // LINE: 808
        lno += 1;
        cno = 0; // LINE: 810
      }
      ParserState nextState; // LINE: 812
      try { // LINE: 813
        Map.Entry<ParserState, Object> nextState_value = state.consume(c, value); // LINE: 814
        nextState = nextState_value.getKey();
        value = nextState_value.getValue();
      } catch (NotationError e) { // LINE: 815
        throw new ParserError(e, lno, cno); // LINE: 816
      }
      cno += 1;
      assert nextState != null;
      state = nextState; // LINE: 821
    }
    Map.Entry<ParserState, Object> endstate_result = state.consume(EOF, value); // LINE: 823
    ParserState endstate = endstate_result.getKey();
    Object result = endstate_result.getValue();
    return result; // LINE: 825
  }
}