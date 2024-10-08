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

import static trld.platform.Common.jsonEncode;
import trld.platform.Input;
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
import static trld.jsonld.Star.ANNOTATION;
import static trld.jsonld.Star.ANNOTATED_TYPE_KEY;
import static trld.Rdfterms.RDF_TYPE;
import static trld.Rdfterms.XSD;
import static trld.Rdfterms.XSD_DOUBLE;
import static trld.Rdfterms.XSD_INTEGER;


public class Parser {
  public static final String XSD_DECIMAL = XSD + "decimal";
  public static final Set<String> AT_KEYWORDS = new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) PREFIX, BASE})));
  public static final String RQ_PREFIX = "prefix";
  public static final String RQ_BASE = "base";
  public static final String RQ_GRAPH = "graph";
  public static final Set<String> RQ_KEYWORDS = new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) RQ_PREFIX, RQ_BASE, RQ_GRAPH})));
  public static final Map<String, String> ESC_CHARS = Builtins.mapOf("t", "\t", "b", "", "n", "\n", "r", "\r", "f", "\u000c", "\"", "\"", "'", "'", "\\", "\\");
  public static final Set<String> RESERVED_CHARS = new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) "~", ".", "-", "!", "$", "&", "'", "(", ")", "*", "+", ",", ";", "=", "/", "?", "#", "@", "%", "_"})));
  public static final Pattern NUMBER_LEAD_CHARS = (Pattern) Pattern.compile("[+-.0-9]");
  public static final Pattern TURTLE_INT_CHARS = (Pattern) Pattern.compile("[+-.0-9]");
  public static final Set<String> LITERAL_QUOTE_CHARS = new HashSet(new ArrayList<>(Arrays.asList(new String[] {(String) "\"", "'"})));
  public static final String SYMBOL = "@symbol";
  public static final String EOF = "";
  public static Object parse(Input inp) {
    ParserState state = new ReadNodes(null);
    Object value = null;
    Integer lno = 1;
    Integer cno = 1;
    for (String c : ((Iterable<String>) inp.characters())) {
      if ((c == null && ((Object) "\n") == null || c != null && (c).equals("\n"))) {
        lno += 1;
        cno = 0;
      }
      ParserState nextState;
      try {
        Map.Entry<ParserState, Object> nextState_value = state.consume(c, value);
        nextState = nextState_value.getKey();
        value = nextState_value.getValue();
      } catch (NotationError e) {
        throw new ParserError(e, lno, cno);
      }
      cno += 1;
      assert nextState != null;
      state = nextState;
    }
    Map.Entry<ParserState, Object> endstate_result = state.consume(EOF, value);
    ParserState endstate = endstate_result.getKey();
    Object result = endstate_result.getValue();
    return result;
  }
}
