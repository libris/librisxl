package whelk.search2.querytree;

public sealed abstract class Resource implements Value permits Link, VocabTerm {
    public abstract String getType();
}