package whelk.sru.cql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import sru.whelk.cql.cqlLexer;
import sru.whelk.cql.cqlParser;

public class Translation
{
    public static String translateCqlToXlQuery(String cqlQuery) {

        // Parse (using ANTLR)
        cqlLexer lexer = new cqlLexer( CharStreams.fromString(cqlQuery) );
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        cqlParser parser = new cqlParser(tokens);
        cqlParser.CqlContext cst = parser.cql();

        // Translate
        Object phase1Ast = Phase1.reduce(cst);

        System.err.println(cqlQuery);
        System.err.println(phase1Ast);
        System.err.println("--------------");


        return null;
    }


}