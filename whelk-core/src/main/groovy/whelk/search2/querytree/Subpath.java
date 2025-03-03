package whelk.search2.querytree;

public sealed interface Subpath permits Key, Property {
    Key key();
    boolean isType();
    boolean isValid();
    @Override
    String toString();
}