/**
 * This file was automatically generated by the TRLD transpiler.
 * Source: trld/trig/serializer.py
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

import static trld.platform.Common.uuid4;
import trld.platform.Output;
import static trld.jsonld.Base.BASE;
import static trld.jsonld.Base.CONTAINER;
import static trld.jsonld.Base.CONTEXT;
import static trld.jsonld.Base.GRAPH;
import static trld.jsonld.Base.ID;
import static trld.jsonld.Base.INDEX;
import static trld.jsonld.Base.LANGUAGE;
import static trld.jsonld.Base.LIST;
import static trld.jsonld.Base.NONE;
import static trld.jsonld.Base.PREFIX;
import static trld.jsonld.Base.PREFIX_DELIMS;
import static trld.jsonld.Base.REVERSE;
import static trld.jsonld.Base.TYPE;
import static trld.jsonld.Base.VALUE;
import static trld.jsonld.Base.VOCAB;
import static trld.jsonld.Star.ANNOTATION;
import static trld.jsonld.Star.ANNOTATED_TYPE_KEY;
import static trld.trig.Serializer.*;


public class Settings {
  public Boolean turtleOnly = false;
  public Boolean turtleDropNamed = false;
  public Boolean dropRdfstar = false;
  public String indentChars = "  ";
  public Boolean useGraphKeyword = true;
  public Boolean upcaseKeywords = false;
  public Boolean predicateRepeatNewLine = true;
  public Boolean bracketEndNewLine = false;
  public Integer prologueEndLine = 1;
  public Settings() {
    this(false, false, false, "  ", true, false, true, false, 1);
  }
  public Settings(Boolean turtleOnly) {
    this(turtleOnly, false, false, "  ", true, false, true, false, 1);
  }
  public Settings(Boolean turtleOnly, Boolean turtleDropNamed) {
    this(turtleOnly, turtleDropNamed, false, "  ", true, false, true, false, 1);
  }
  public Settings(Boolean turtleOnly, Boolean turtleDropNamed, Boolean dropRdfstar) {
    this(turtleOnly, turtleDropNamed, dropRdfstar, "  ", true, false, true, false, 1);
  }
  public Settings(Boolean turtleOnly, Boolean turtleDropNamed, Boolean dropRdfstar, String indentChars) {
    this(turtleOnly, turtleDropNamed, dropRdfstar, indentChars, true, false, true, false, 1);
  }
  public Settings(Boolean turtleOnly, Boolean turtleDropNamed, Boolean dropRdfstar, String indentChars, Boolean useGraphKeyword) {
    this(turtleOnly, turtleDropNamed, dropRdfstar, indentChars, useGraphKeyword, false, true, false, 1);
  }
  public Settings(Boolean turtleOnly, Boolean turtleDropNamed, Boolean dropRdfstar, String indentChars, Boolean useGraphKeyword, Boolean upcaseKeywords) {
    this(turtleOnly, turtleDropNamed, dropRdfstar, indentChars, useGraphKeyword, upcaseKeywords, true, false, 1);
  }
  public Settings(Boolean turtleOnly, Boolean turtleDropNamed, Boolean dropRdfstar, String indentChars, Boolean useGraphKeyword, Boolean upcaseKeywords, Boolean predicateRepeatNewLine) {
    this(turtleOnly, turtleDropNamed, dropRdfstar, indentChars, useGraphKeyword, upcaseKeywords, predicateRepeatNewLine, false, 1);
  }
  public Settings(Boolean turtleOnly, Boolean turtleDropNamed, Boolean dropRdfstar, String indentChars, Boolean useGraphKeyword, Boolean upcaseKeywords, Boolean predicateRepeatNewLine, Boolean bracketEndNewLine) {
    this(turtleOnly, turtleDropNamed, dropRdfstar, indentChars, useGraphKeyword, upcaseKeywords, predicateRepeatNewLine, bracketEndNewLine, 1);
  }
  public Settings(Boolean turtleOnly, Boolean turtleDropNamed, Boolean dropRdfstar, String indentChars, Boolean useGraphKeyword, Boolean upcaseKeywords, Boolean predicateRepeatNewLine, Boolean bracketEndNewLine, Integer prologueEndLine) {
    this.turtleOnly = turtleOnly;
    this.turtleDropNamed = turtleDropNamed;
    this.dropRdfstar = dropRdfstar;
    this.indentChars = indentChars;
    this.useGraphKeyword = useGraphKeyword;
    this.upcaseKeywords = upcaseKeywords;
    this.predicateRepeatNewLine = predicateRepeatNewLine;
    this.bracketEndNewLine = bracketEndNewLine;
    this.prologueEndLine = prologueEndLine;
  }
}
