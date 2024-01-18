package whelk.xlql;

public class Analysis {
    static Object flattenCodeEquals(Object parent, Object astNode) {
        // Flatten CodeEquals-objects into sequences of the contained parts.
        // Example: publisher:(A or B) instead becomes publisher:A or publisher:B

        if (astNode instanceof Ast.And) {
            flattenCodeEquals(astNode, ((Ast.And) astNode).operands());
        } else if (astNode instanceof Ast.Or) {
            flattenCodeEquals(astNode, ((Ast.Or) astNode).operands());
        } else if (astNode instanceof Ast.Not) {
            flattenCodeEquals(astNode, ((Ast.Not) astNode).operand());
        } else if (astNode instanceof Ast.Like) {
            flattenCodeEquals(astNode, ((Ast.Like) astNode).operand());
        } else if (astNode instanceof Ast.CodeLesserGreaterThan) {
        } else if (astNode instanceof Ast.CodeEquals) {
            //flattenCodeEquals(astNode.)
        }
        return null;
    }

    static void checkSemantics(Object astNode) throws BadQueryException {
        checkNoCodeWithinCode(astNode, false);
    }

    static void checkNoCodeWithinCode(Object astNode, boolean inCodeGroup) throws BadQueryException {
        if (astNode instanceof Ast.And) {
            for (Object child : ((Ast.And) astNode).operands()) {
                checkNoCodeWithinCode(child, inCodeGroup);
            }
        } else if (astNode instanceof Ast.Or) {
            for (Object child : ((Ast.Or) astNode).operands()) {
                checkNoCodeWithinCode(child, inCodeGroup);
            }
        } else if (astNode instanceof Ast.Not) {
            checkNoCodeWithinCode(((Ast.Not) astNode).operand(), inCodeGroup);
        } else if (astNode instanceof Ast.Like) {
            checkNoCodeWithinCode(((Ast.Like) astNode).operand(), inCodeGroup);
        } else if (astNode instanceof Ast.CodeLesserGreaterThan) {
            if (inCodeGroup)
                throw new BadQueryException("Codes within code groups are not allowed.");
        } else if (astNode instanceof Ast.CodeEquals) {
            if (inCodeGroup)
                throw new BadQueryException("Codes within code groups are not allowed.");
            else
                checkNoCodeWithinCode(((Ast.CodeEquals) astNode).operand(), true);
        }

    }
}
