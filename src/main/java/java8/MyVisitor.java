package java8;

public class MyVisitor extends Java8BaseVisitor {
    @Override
    public Object visitForStatement(Java8Parser.ForStatementContext ctx) {
        System.out.printf("I am visiting %s's for statement\n",ctx.getText());
        return 1;
    }
}
