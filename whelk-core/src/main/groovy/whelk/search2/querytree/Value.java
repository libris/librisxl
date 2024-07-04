package whelk.search2.querytree;

public sealed interface Value permits Link, Literal, VocabTerm {
    String string();

    default String canonicalForm() {
        return string();
    }

    default boolean isNumeric() {
        return false;
    }

    default Value increment() {
        return this;
    }

    default Value decrement() {
        return this;
    }
}