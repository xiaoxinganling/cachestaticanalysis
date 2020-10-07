package java8;

public class MyListener extends Java8BaseListener {
    @Override
    public void enterAssignment(Java8Parser.AssignmentContext ctx) {
        System.out.println(ctx.getText());
    }

    @Override
    public void enterBlock(Java8Parser.BlockContext ctx) {
        System.out.println(ctx.getText());
    }

}
