package whelk.search2.querytree;

public sealed interface Value permits Link, Literal, InvalidValue, VocabTerm {
    String string();

    Object description();

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