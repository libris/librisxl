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
import static trld.trig.Parser.*;


public class ConsumeWs extends BaseParserState { // LINE: 104
  ConsumeWs(/*@Nullable*/ ParserState parent) { super(parent); };
  public static final Pattern MATCH = (Pattern) Pattern.compile("\\s"); // LINE: 106

  public boolean accept(String c) { // LINE: 108
    return (this.MATCH.matcher(c).matches() ? c : null) != null; // LINE: 109
  }

  public Map.Entry<ParserState, Object> consume(String c, Object prevValue) { // LINE: 111
    if (this.accept(c)) { // LINE: 112
      return new KeyValue(this, null); // LINE: 113
    } else {
      return this.parent.consume(c, prevValue); // LINE: 115
    }
  }
}