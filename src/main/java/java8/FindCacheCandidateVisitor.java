package java8;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class FindCacheCandidateVisitor extends Java8BaseVisitor{
    // mark all RDDs
    public Map<String, List<RDD>> rdds = new HashMap<String, List<RDD>>();

    // deal with local variable declaration
    @Override
    public Object visitLocalVariableDeclaration(Java8Parser.LocalVariableDeclarationContext ctx) {
        String type = ctx.unannType().getText();
        if(Pattern.matches(CacheUtils.RDDPATTERN,type)){
            // TODO: add more check about type, `MyVisitor:72-77`
            Java8Parser.VariableDeclaratorListContext vars = ctx.variableDeclaratorList();
            for(int i = 0; i < ctx.variableDeclaratorList().getChildCount(); i++){
                // TODO: these codes need to consider about iteration condition
                Java8Parser.VariableDeclaratorContext var = vars.variableDeclarator(i);
                String id = var.variableDeclaratorId().getText();
                int line = ctx.start.getLine();
                addRDD(new RDD(id,line,0,-1));
                // find the RDDs in the right of `=`
                List<Token> tks = dfsToFindRDDs(var.variableInitializer(),102);
                for(Token t: tks){
                    updateRDD(t.getText());
                }
                if(var.variableInitializer()!=null){
                    super.visitVariableInitializer(var.variableInitializer());
                }
            }
            return 0;
        }
        return super.visitLocalVariableDeclaration(ctx);
    }

    @Override
    public Object visitAssignment(Java8Parser.AssignmentContext ctx) {
        if(ctx.leftHandSide().expressionName()!=null&&
        isContained(ctx.leftHandSide().getText())){
            // firstly update parent
            List<Token> tks = dfsToFindRDDs(ctx.expression(),102);
            for(Token t:tks){
                updateRDD(t.getText());
            }
            RDD tmp = new RDD(ctx.leftHandSide().getText(),ctx.start.getLine(),
                    0,-1);
            addRDD(tmp);
            return super.visitExpression(ctx.expression());
        }
        return super.visitAssignment(ctx);
    }

    // deal with action

    @Override
    public Object visitMethodInvocation(Java8Parser.MethodInvocationContext ctx) {
        if(ctx.Identifier()!=null&&
                Pattern.matches(CacheUtils.actionPattern,ctx.Identifier().getText())){
            // need to update RDD
            updateRDDsInTree((ParserRuleContext) ctx.getChild(0));
            return 0;// TODO: need to concern about other items of `MethodInvocation`
        }
        return super.visitMethodInvocation(ctx);
    }

    @Override
    public Object visitMethodInvocation_lf_primary(Java8Parser.MethodInvocation_lf_primaryContext ctx) {
        if(Pattern.matches(CacheUtils.actionPattern,ctx.Identifier().getText())){
            if(Java8Parser.PrimaryContext.class == ctx.getParent().getParent().getClass()){
                updateRDDsInTree(ctx.getParent().getParent());
                return 0;
            }
        }
        return super.visitMethodInvocation_lf_primary(ctx);
    }

    @Override
    public Object visitMethodInvocation_lfno_primary(Java8Parser.MethodInvocation_lfno_primaryContext ctx) {
        if(ctx.Identifier()!=null&&
                Pattern.matches(CacheUtils.actionPattern,ctx.Identifier().getText())){
            // need to update RDD
            updateRDDsInTree((ParserRuleContext) ctx.getChild(0));
            return 0;// TODO: need to concern about other items of `MethodInvocation`
        }
        return super.visitMethodInvocation_lfno_primary(ctx);
    }

    // deal with iteration

    @Override
    public Object visitDoStatement(Java8Parser.DoStatementContext ctx) {
        super.visitDoStatement(ctx);
        return super.visitDoStatement(ctx);
    }

    @Override
    public Object visitForStatement(Java8Parser.ForStatementContext ctx) {
        super.visitForStatement(ctx);
        return super.visitForStatement(ctx);
    }

    @Override
    public Object visitWhileStatement(Java8Parser.WhileStatementContext ctx) {
        super.visitWhileStatement(ctx);
        return super.visitWhileStatement(ctx);
    }

    // select cache candidate from `rdds`
    public List<RDD> findToBeCache(){
        List<RDD> res = new ArrayList<RDD>();
        for(String key: rdds.keySet()){
            List<RDD> tmp = rdds.get(key);
            RDD last = null;
            for(RDD rdd: tmp){
                if(last==null||!(last.id.equals(rdd.id)&&last.line==rdd.line)){
                    if(rdd.outgoingEdge>=2){
                        res.add(rdd);
                        last = rdd;
                    }
                }
            }
        }
        return res;
    }

    // update RDD in an parseTree
    private void updateRDDsInTree(ParserRuleContext tree){
        List<Token> tks = dfsToFindRDDs(tree,102);
        for(Token t: tks){
            updateRDD(t.getText());
        }
    }

    // add RDD item ito `rdds`
    private void addRDD(RDD cur){
        String key = cur.id;
        if(rdds.containsKey(key)){
            rdds.get(key).add(cur);
        }else{
            List<RDD> tmp = new ArrayList<RDD>();
            tmp.add(cur);
            rdds.put(key,tmp);
        }
    }

    // update RDD's outgoing edges
    private void updateRDD(String id){
        if(rdds.containsKey(id)){
            List<RDD> tmp = rdds.get(id);
            tmp.get(tmp.size()-1).outgoingEdge++;
        }
    }

    // judge if the rdd contains the id
    private boolean isContained(String id){
        return rdds.containsKey(id);
    }

    // find rdd that declared before
    // TODO: need to concern about the scope
    // TODO: we can use TypeName or ExpressionName to replace the function
    private List<Token> dfsToFindRDDs(ParserRuleContext node, int type){
        if(node.getTokens(type).size()>0&&
                node.getChild(0).getChildCount()==0&&
                (Java8Parser.TypeNameContext.class == node.getClass()||
                        Java8Parser.ExpressionNameContext.class == node.getClass())&&
                isContained(node.getChild(0).getText())){// TODO: add the filter
            // save variables declared before
            // **rdd name sometimes will be typename, some times will be expression name**
            List<Token> tks = new ArrayList<Token>();
            tks.add(node.getTokens(type).get(0).getSymbol());
            return tks;
        }
        List<Token> tks = new ArrayList<Token>();
        for(int i=0;i<node.getChildCount();i++){
            if(node.getChild(i).getChildCount()>0){
               tks.addAll(dfsToFindRDDs((ParserRuleContext) node.getChild(i), type));
            }
        }
        return tks;
    }
}
// An abstraction of RDD containing <id, line, outgoing edge, iteration>
class RDD{
    public String id;
    public int line;
    public int outgoingEdge;
    public int iteration; // TODO: iteration will be helpful in near future
    // TODO: update cache candidate's actions
    public RDD(String id, int line, int outgoingEdge, int iteration) {
        this.id = id;
        this.line = line;
        this.outgoingEdge = outgoingEdge;
        this.iteration = iteration;
    }

    @Override
    public String toString() {
        return String.format("<id: %s, line: %d, outgoing Line: %d, iteration: %d>"
                ,id,line, outgoingEdge,iteration);
    }
}
