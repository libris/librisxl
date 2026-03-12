package whelk.sru.cql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import sru.whelk.cql.cqlLexer;
import sru.whelk.cql.cqlParser;

import java.util.ArrayList;
import java.util.List;

public class Translation
{
    public static String translateCqlToXlQuery(String cqlQuery) {
        cqlLexer lexer = new cqlLexer( CharStreams.fromString(cqlQuery) );
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        cqlParser parser = new cqlParser(tokens);
        cqlParser.CqlContext cst = parser.cql();

        Object ast = reduce(cst.sortedQuery());

        System.err.println(cqlQuery);
        System.err.println(ast);
        System.err.println("--------------");


        return null;
    }

    private record AstModifier(String name, String comparitor, String compareTo) {}
    private record AstBooleanGroup(String op, List<AstModifier> modifiers) {}
    private record AstRelation(String comparitor, List<AstModifier> modifiers) {}
    private record AstSearchClause(String index, AstRelation relation, String term) {}
    private record AstScopedClause(Object scopedClause, AstBooleanGroup booleanGroup, Object searchClause ) {}

    private static Object reduce(cqlParser.CqlContext cql) {
        return reduce(cql.sortedQuery());
    }

    private static Object reduce(cqlParser.SortedQueryContext sortedQuery) {
        // sortedQuery 	::= 	prefixAssignment sortedQuery | scopedClause ['sortby' sortSpec]

        // ignore sortedQuery.prefixAssignment()
        // ignore sortedQuery.sortSpec()
        if (sortedQuery.sortedQuery() != null) {
            return reduce(sortedQuery.sortedQuery());
        }
        else {
            return reduce(sortedQuery.scopedClause());
        }
    }

    private static Object reduce(cqlParser.ScopedClauseContext scopedClause) {
        // scopedClause 	::= 	scopedClause booleanGroup searchClause | searchClause

        if (scopedClause.booleanGroup() != null) {
            return new AstScopedClause( reduce(scopedClause.scopedClause()), reduce(scopedClause.booleanGroup()), reduce(scopedClause.searchClause()) );
        } else {
            return reduce(scopedClause.searchClause());
        }
    }

    private static Object reduce(cqlParser.SearchClauseContext searchClause) {
        // searchClause 	::= 	'(' cqlQuery ')' | index relation searchTerm | searchTerm

        if (searchClause.cqlQuery() != null) {
            return reduce(searchClause.cqlQuery());
        } else if (searchClause.index() != null) {
            return new AstSearchClause( reduce(searchClause.index()), reduce(searchClause.relation()), reduce(searchClause.searchTerm()));
        } else {
            return reduce(searchClause.searchTerm());
        }
    }

    private static Object reduce(cqlParser.CqlQueryContext cqlQuery) {
        // cqlQuery 	::= 	prefixAssignment cqlQuery | scopedClause
        if (cqlQuery.scopedClause() != null) {
            return reduce(cqlQuery.scopedClause());
        } else {
            // ignore cqlQuery.prefixAssignment()
            return reduce(cqlQuery.cqlQuery());
        }
    }

    private static AstRelation reduce(cqlParser.RelationContext relation) {
        // relation 	::= 	comparitor [modifierList]

        if (relation.modifierList() != null)
            return new AstRelation( reduce(relation.comparitor()), reduce(relation.modifierList()) );
        else
            return new AstRelation( reduce(relation.comparitor()), null );
    }

    private static String reduce(cqlParser.ComparitorContext comparitor) {
        if (comparitor.comparitorSymbol() != null) {
            return reduce(comparitor.comparitorSymbol());
        } else {
            return reduce(comparitor.namedComparitor());
        }
    }

    private static String reduce(cqlParser.NamedComparitorContext namedComparitor) {
        return reduce( namedComparitor.identifier() );
    }

    private static String reduce(cqlParser.SearchTermContext searchTerm) {
        return reduce( searchTerm.term() );
    }

    private static String reduce(cqlParser.IndexContext index) {
        return reduce( index.term() );
    }

    private static AstBooleanGroup reduce(cqlParser.BooleanGroupContext booleanGroup) {
        // booleanGroup 	::= 	boolean [modifierList]

        String op;
        if (booleanGroup.boolean_().AND() != null) {
            op = "and";
        } else if (booleanGroup.boolean_().OR() != null) {
            op = "or";
        } else if (booleanGroup.boolean_().NOT() != null) {
            op = "not";
        } else {
            op = "prox";
        }

        if (booleanGroup.modifierList() == null) {
            return new AstBooleanGroup(op, null);
        } else {
            return new AstBooleanGroup(op, reduce(booleanGroup.modifierList()));
        }
    }

    private static List<AstModifier> reduce(cqlParser.ModifierListContext modifierList) {
        // modifierList 	::= 	modifierList modifier | modifier

        List<AstModifier> modifiers = new ArrayList<>();
        if (modifierList.modifierList() != null) {
            modifiers.addAll( reduce(modifierList.modifierList()) );
        }
        modifiers.add( reduce(modifierList.modifier()) );
        return modifiers;
    }

    private static AstModifier reduce(cqlParser.ModifierContext modifier) {
        // modifier 	::= 	'/' modifierName [comparitorSymbol modifierValue]

        String modValue = null;
        String modComparitor = null;
        if (modifier.modifierValue() != null) {
            modValue = reduce( modifier.modifierValue() );
            modComparitor = reduce( modifier.comparitorSymbol() );
        }
        return new AstModifier( reduce(modifier.modifierName()), modComparitor, modValue );
    }

    private static String reduce(cqlParser.ComparitorSymbolContext comparitorSymbol) {
        // comparitorSymbol 	::= 	'=' | '>' | '<' | '>=' | '<=' | '<>' | '=='

        return comparitorSymbol.getText();
    }

    private static String reduce(cqlParser.ModifierValueContext modifierValue) {
        return reduce(modifierValue.term());
    }

    private static String reduce(cqlParser.TermContext term) {
        return reduce( term.identifier() );
    }

    private static String reduce(cqlParser.IdentifierContext identifier) {
        return identifier.getText();
    }

    private static String reduce(cqlParser.ModifierNameContext modifierName) {
        return reduce(modifierName.term());
    }
}