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


public class ReadGraph extends ReadNodes { // LINE: 775
  ReadGraph(/*@Nullable*/ ParserState parent) { super(parent); };

  public Map.Entry<ParserState, Object> consume(String c, Object prevValue) { // LINE: 777
    if ((prevValue instanceof String && !prevValue.equals(TYPE))) { // LINE: 778
      throw new NotationError("Directive not allowed in graph: " + prevValue); // LINE: 779
    }
    ReadNodes readnodes = (ReadNodes) ((ReadNodes) this.parent); // LINE: 781
    if ((this.expectGraph || (this.openBrace && !c.equals("|")))) { // LINE: 783
      throw new NotationError("Nested graphs are not allowed in TriG"); // LINE: 784
    }
    if ((c == null && ((Object) "}") == null || c != null && (c).equals("}"))) { // LINE: 786
      if (this.node != null) { // LINE: 787
        if ((this.p != null && prevValue != null)) { // LINE: 788
          this.fillNode(prevValue); // LINE: 789
        }
        this.nextNode(); // LINE: 790
      }
      if (readnodes.node == null) { // LINE: 791
        readnodes.nodes.addAll(this.nodes);
      } else {
        readnodes.node.put(GRAPH, this.nodes); // LINE: 794
        readnodes.nextNode(); // LINE: 795
      }
      return new KeyValue(readnodes, null); // LINE: 796
    } else {
      return super.consume(c, prevValue); // LINE: 798
    }
  }
}