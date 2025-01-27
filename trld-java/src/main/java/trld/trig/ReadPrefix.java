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
import static trld.trig.Parser.*;


public class ReadPrefix extends ReadDecl {
  public ReadPrefix(ReadNodes parent, Boolean finalDot) { super(parent, finalDot); };
  public /*@Nullable*/ String pfx;
  public /*@Nullable*/ String ns;

  public void init() {
    this.pfx = null;
    this.ns = null;
  }

  public boolean moreParts(Map value) {
    if (this.pfx == null) {
      String pfx = (String) ((String) value.get(SYMBOL));
      if (!pfx.equals("")) {
        if (pfx.endsWith(":")) {
          pfx = (pfx.length() >= 0 ? pfx.substring(0, pfx.length() - 1) : "");
        } else {
          throw new NotationError("Invalid prefix " + pfx);
        }
      }
      this.pfx = pfx;
      return true;
    }
    if (this.ns == null) {
      this.ns = (String) value.get(ID);
    }
    return false;
  }

  public void declare() {
    assert this.ns != null;
    Object ns = (Object) this.ns;
    if ((!this.pfx.equals("") && !this.ns.equals("") && !(PREFIX_DELIMS.contains(this.ns.substring(this.ns.length() - 1, this.ns.length() - 1 + 1))))) {
      ns = Builtins.mapOf(ID, this.ns, PREFIX, true);
    }
    String key = ((this.pfx != null && !this.pfx.equals("")) ? this.pfx : VOCAB);
    this.parent.context.put(key, ns);
  }
}
