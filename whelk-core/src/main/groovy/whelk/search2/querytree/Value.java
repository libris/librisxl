package whelk.search2.querytree;

public sealed interface Value permits InvalidValue, Literal, Resource {
    Object description();

    // Input form
    String raw();

    // As represented in indexed docs
    String jsonForm();

    // "Pretty" form
    @Override
    String toString();
}