package java8;

public class MyListener extends Java8BaseListener {
    @Override
    public void enterForStatement(Java8Parser.ForStatementContext ctx) {
        System.out.printf("I am visiting %s's for statement\n",ctx.getText());
    }

    @Override
    public void exitForStatement(Java8Parser.ForStatementContext ctx) {
        System.out.printf("I am exiting %s's for statement\n",ctx.getText());
    }
}
