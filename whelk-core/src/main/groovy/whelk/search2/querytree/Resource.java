package whelk.search2.querytree;

import java.util.Map;

public sealed abstract class Resource implements Value permits Link, VocabTerm, InvalidValue {
    public abstract String getType();

    public abstract Map<String, Object> description();

    // As represented in indexed docs
    public abstract String jsonForm();
}