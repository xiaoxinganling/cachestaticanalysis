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
            }
        }
        return 0;
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
    public int iteration;
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
