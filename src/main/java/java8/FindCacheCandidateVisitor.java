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
    // save all actions with the timeline
    public List<Token> actions = new ArrayList<Token>();

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
                RDD left = new RDD(id,line,0,-1);
                // find the RDDs in the right of `=`
                List<Token> tks = dfsToFindRDDs(var.variableInitializer(),102);
                for(Token t: tks){
                    updateRDD(t.getText(),left);
                }
                addRDD(left);
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
            RDD left = new RDD(ctx.leftHandSide().getText(),ctx.start.getLine(),
                    0,-1);
            // firstly update parent
            List<Token> tks = dfsToFindRDDs(ctx.expression(),102);
            for(Token t:tks) {
                updateRDD(t.getText(), left);
            }
            addRDD(left);
            return super.visitExpression(ctx.expression());
        }
        return super.visitAssignment(ctx);
    }

    // start dealing with action
    @Override
    public Object visitMethodInvocation(Java8Parser.MethodInvocationContext ctx) {
        if(ctx.Identifier()!=null&&
                Pattern.matches(CacheUtils.actionPattern,ctx.Identifier().getText())){
            // need to update RDD
            updateRDDsInTree((ParserRuleContext) ctx.getChild(0),ctx.Identifier().getSymbol());
            addAction(ctx.Identifier().getSymbol());
            return 0;// TODO: need to concern about other items of `MethodInvocation`
        }
        return super.visitMethodInvocation(ctx);
    }

    @Override
    public Object visitMethodInvocation_lf_primary(Java8Parser.MethodInvocation_lf_primaryContext ctx) {
        if(Pattern.matches(CacheUtils.actionPattern,ctx.Identifier().getText())){
            if(Java8Parser.PrimaryContext.class == ctx.getParent().getParent().getClass()){
                updateRDDsInTree(ctx.getParent().getParent(),ctx.Identifier().getSymbol());
                addAction(ctx.Identifier().getSymbol());
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
            updateRDDsInTree((ParserRuleContext) ctx.getChild(0),ctx.Identifier().getSymbol());
            addAction(ctx.Identifier().getSymbol());
            return 0;// TODO: need to concern about other items of `MethodInvocation`
        }
        return super.visitMethodInvocation_lfno_primary(ctx);
    }
    private void addAction(Token action){
        actions.add(action);
    }
    // end dealing action

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
                // TODO: need to remove duplication?
                //if(last==null||!(last.id.equals(rdd.id)&&last.line==rdd.line)){
                    if(rdd.outgoingEdge>=2){
                        res.add(rdd);
                        last = rdd;
                    }
                //}
            }
        }
        return res;
    }
    // use dfs to determine an RDD's subsequent actions
    public void findAfterAction(List<RDD> candidates){
        for(RDD curCache: candidates){
            Map<String,Integer> rddMap = new HashMap<String,Integer>();
            dfsRDD(curCache,curCache,rddMap);
        }
    }
    // do the dfs
    private void dfsRDD(RDD curRDD,RDD toBeUpdate, Map<String,Integer> rddMap){
        if(hasVisited(rddMap,curRDD)){
            return;
        }
        if(curRDD.isAction!=null){
            toBeUpdate.actions.add(curRDD.isAction);
        }else{
            for(RDD child: curRDD.children){
                dfsRDD(child,toBeUpdate,rddMap);
            }
        }
        markVisited(rddMap,curRDD);
    }
    // to judge whether the RDD has been visited
    private boolean hasVisited(Map<String,Integer> rddMap, RDD curRDD){
        String key = curRDD.id + "_" +
                curRDD.line + "_" +
                curRDD.iteration;
        return rddMap.containsKey(key);
    }
    // to mark the node has been visited
    private void markVisited(Map<String,Integer> rddMap, RDD curRDD){
        if(curRDD.isAction!=null){
            return;
        }
        String key = curRDD.id + "_" +
                curRDD.line + "_" +
                curRDD.iteration;
        if(!hasVisited(rddMap,curRDD)){
            rddMap.put(key,1);
        }
    }

    // update RDD in an parseTree
    private void updateRDDsInTree(ParserRuleContext tree,Token action){
        List<Token> tks = dfsToFindRDDs(tree,102);
        // use id='0' to indicate the RDD is an action
        RDD curAction = new RDD("0"+action.getText(),action.getLine(),0,0);
        curAction.isAction = action;
        for(Token t: tks){
            updateRDD(t.getText(),curAction);
        }
    }

    // add RDD item ito `rdds`
    private void addRDD(RDD cur){
        String key = cur.id;
        if(rdds.containsKey(key)){
            // we need to update RDD's iteration
            List<RDD> tmp = rdds.get(key);
            RDD last = tmp.get(tmp.size()-1);
            if(last.id.equals(cur.id) && last.line==cur.line){
                cur.iteration = last.iteration+1;
            }
            rdds.get(key).add(cur);
        }else{
            List<RDD> tmp = new ArrayList<RDD>();
            tmp.add(cur);
            rdds.put(key,tmp);
        }
    }

    // update RDD's outgoing edges and children
    private void updateRDD(String source, RDD child){
        if(rdds.containsKey(source)){
            List<RDD> tmp = rdds.get(source);
            RDD sourceRDD = tmp.get(tmp.size()-1);
            sourceRDD.outgoingEdge++;
            // TODO :we need to remove duplicate children
            // however it may cause confusion
//            boolean isDuplicate = false;
//            for(RDD curChild : sourceRDD.children){
//                if(curChild.equals(child)){
//                    isDuplicate = true;
//                }
//            }
//            if(!isDuplicate){
//                sourceRDD.children.add(child);
//            }
            sourceRDD.children.add(child);
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
    public List<RDD> children;
    public Token isAction;// TODO: we can't bear such ugly method to add action to an RDD
    public List<Token> actions;
    public RDD(String id, int line, int outgoingEdge, int iteration) {
        this.id = id;
        this.line = line;
        this.outgoingEdge = outgoingEdge;
        this.iteration = iteration;
        this.children = new ArrayList<RDD>();
        this.actions = new ArrayList<Token>();
        this.isAction = null;
    }

    @Override
    public String toString() {
        StringBuilder children = new StringBuilder();
        children.append("[");
        for(RDD child:this.children){
            children.append(child.id).append("_").append(child.line).append(",");
        }
        children.deleteCharAt(children.length()-1);
        children.append("]");
        StringBuilder actions = new StringBuilder();
        actions.append("[");
        for(Token action:this.actions){
            actions.append(action.getText()).append("_").append(action.getLine()).append(",");
        }
        actions.deleteCharAt(actions.length()-1);
        actions.append("]");
        return String.format("<id: %s, line: %d, outgoing Line: %d, iteration: %d, children: %s, isAction: %s, actions: %s>"
                ,id,line, outgoingEdge,iteration,children.toString(),isAction,actions.toString());
    }
}
