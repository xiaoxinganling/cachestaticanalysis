package java8;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

// 自定义顺序的意思就是，如果我访问了这个，这里面的逻辑需要自己来写
public class MyVisitor extends Java8BaseVisitor {
    // declared RDD
    public Map<String,Declaration> identifiers = new HashMap<String, Declaration>();

    // RDD reside left from the '='
    public List<List<Token>> left = new ArrayList<List<Token>>();

    // RDD reside right from the '='
    public List<List<Token>> right = new ArrayList<List<Token>>();

    // RDD need to cache in iteration pattern
    public List<CacheAdvice> toCache = new ArrayList<CacheAdvice>();

    // current iteration number
    private int isIteration = 0;

    // get RDD's appearance
    public Map<String,RDDCallAction> rddAppearance = new HashMap<String, RDDCallAction>();

    // current RDD analysed in multi-action pattern
    private Token curRDD;

    // the start of the first iteration
    private List<Token> iterationFirst = new ArrayList<Token>();

    // TODO ensure current class is the class for analysis

    public String curClass;
//
//    @Override
//    public Object visitNormalClassDeclaration(Java8Parser.NormalClassDeclarationContext ctx) {
//        if(ctx.Identifier().getText().equals(curClass)){
//            //super.visitNormalClassDeclaration(ctx);
//        }
//        return super.visitNormalClassDeclaration(ctx);
//    }

    // Step 1. find all declared RDD：<type,name,line>
    @Override
    public Object visitLocalVariableDeclaration(Java8Parser.LocalVariableDeclarationContext ctx){
        // get type
        String type = ctx.unannType().getText();
        Java8Parser.VariableDeclaratorListContext variables = ctx.variableDeclaratorList();
        if(isIteration>0){
            // step 2, find iteration pattern
            for(int i = 0; i < variables.getChildCount(); i++){
                Java8Parser.VariableDeclaratorContext variable = variables.variableDeclarator(i);
                if(variable.getText().contains("=")){
                    //System.out.println("(local var)left: "+variable.variableDeclaratorId().getText());
                    addLeft(isIteration,variable.variableDeclaratorId().start);
                    // variableInitializer
                    //	:	expression :need to consider
                    //	|	arrayInitializer :no
                    //	;
                    visitExpression(variable.variableInitializer().expression());
                }
            }
        }else{
            if(isIteration == 0 && Pattern.matches(CacheUtils.pattern,type)) {
                if (type.indexOf('<') != -1) {
                    type = type.substring(0, type.indexOf('<'));
                }
                try {
                    if (Class.forName("org.apache.spark.api.java.JavaRDDLike")
                            .isAssignableFrom(Class.forName("org.apache.spark.api.java." + type))) {
                        // variableDeclaratorList
                        //	:	variableDeclarator (',' variableDeclarator)*
                        //	;
                        for (int i = 0; i < variables.getChildCount(); i++) {
                            // get id and line
                            // variableDeclarator
                            //	:	variableDeclaratorId ('=' variableInitializer)?// mx
                            //	;
                            // (id, <type, id, line, cached>)
                            String rddName = variables.variableDeclarator(i).variableDeclaratorId().getText();
                            Java8Parser.VariableDeclaratorContext curLine = variables.variableDeclarator(i);
                            // stop is good for add our code into the source code
                            identifiers.put(rddName, new Declaration(ctx.unannType().getText(), rddName, ctx.stop.getLine(),
                                    Pattern.matches(CacheUtils.cachePattern,variables.variableDeclarator(i).getText()),ctx.stop.getLine()));
                        }
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            // do action testing
            for (int i = 0; i < variables.getChildCount(); i++) {
                // TODO NULL pointer error
                // long oldCount;
                if (variables.variableDeclarator(i).variableInitializer()!=null
                &&variables.variableDeclarator(i).variableInitializer().expression() != null) {
                    visitExpression(variables.variableDeclarator(i).variableInitializer().expression());
                }
            }
        }
        return 0;
    }

    // Step 2. find iteration pattern


    @Override
    public Object visitDoStatement(Java8Parser.DoStatementContext ctx) {
        //  doStatement
        //	:	'do' statement 'while' '(' expression ')' ';'
        //	;
        // forStatement
        //	:	basicForStatement
        //	|	enhancedForStatement
        //	;
        // basicForStatement
        //	:	'for' '(' forInit? ';' expression? ';' forUpdate? ')' statement
        //	;
        // whileStatement
        //	:	'while' '(' expression ')' statement
        //	;
        if(ctx.getText().contains("=")){
            iterationFirst.add(ctx.start);
            isIteration++;
            findIterationPattern(ctx.statement());
            isIteration--;
            iterationFirst.remove(iterationFirst.size()-1);
        }else{
            super.visitDoStatement(ctx);
        }
        return 0;
    }

    @Override
    public Object visitForStatement(Java8Parser.ForStatementContext ctx) {
        if(ctx.getText().contains("=")){
            iterationFirst.add(ctx.start);
            isIteration++;
            if(ctx.basicForStatement()!=null){
                findIterationPattern(ctx.basicForStatement().statement());
            }else{
                findIterationPattern(ctx.enhancedForStatement().statement());
            }
            isIteration--;
            iterationFirst.remove(iterationFirst.size()-1);
        }else{
            super.visitForStatement(ctx);
        }
        return 0;
    }

    @Override
    public Object visitWhileStatement(Java8Parser.WhileStatementContext ctx) {
        if(ctx.getText().contains("=")){
            iterationFirst.add(ctx.start);
            isIteration++;
            findIterationPattern(ctx.statement());
            isIteration--;
            iterationFirst.remove(iterationFirst.size()-1);
        }else{
            super.visitWhileStatement(ctx);
        }
        return 0;
    }

    private void findIterationPattern(Java8Parser.StatementContext ctx){
        if(right.size()==isIteration) {
            left.set(isIteration-1,new ArrayList<Token>());
            right.set(isIteration-1,new ArrayList<Token>());
        }else{
            left.add(new ArrayList<Token>());
            right.add(new ArrayList<Token>());
        }
        //System.out.println("==============================iteration "+ctx.getText());
        visitBlockStatements(ctx.statementWithoutTrailingSubstatement().block().blockStatements());
        //visitBlockStatements(((Java8Parser.StatementContext)ctx.getChild(ctx.getChildCount()-1)).
        //        statementWithoutTrailingSubstatement().block().blockStatements());
        //System.out.println("==============================");
        for(Token t: filterRight(isIteration)){
            boolean isOccurred = false;
            if(!(identifiers.get(t.getText()).cached&&
                    identifiers.get(t.getText()).cacheLine<iterationFirst.get(0).getLine())){
                for(int j=0;j<toCache.size();j++){
                    CacheAdvice ca = toCache.get(j);
                    if(ca.reason.getText().equals(t.getText())){
                        // get the smaller
                        isOccurred = true;
                        if(ca.adviseLine > iterationFirst.get(0).getLine()){
                            ca.adviseLine = iterationFirst.get(0).getLine();
                            ca.reason = t;
                            // toCache.set(j,ca); TODO: test it
                        }
                        break;
                    }
                }
                if(!isOccurred){
                    toCache.add(new CacheAdvice(t,iterationFirst.get(0).getLine(), CacheUtils.ITERATION));
                }
            }else{
                //System.out.println("cached!!!! "+t.getText());
            }
        }
        left.remove(left.size()-1);
        right.remove(right.size()-1);
    }

    // deal with expression in (local variable declaration | assignment) in for statement
    @Override
    public Object visitExpression(Java8Parser.ExpressionContext ctx) {
        // expression
        //	:	lambdaExpression
        //	|	assignmentExpression
        //	;
        if(isIteration > 0){
            if(ctx.lambdaExpression()!=null){
                visitLambdaExpression(ctx.lambdaExpression());
            }else{
                visitAssignmentExpression(ctx.assignmentExpression());
            }
        }else{
            super.visitExpression(ctx);
        }
        return 0;
    }

    // deal with RDD in assignment expression
    @Override
    public Object visitAssignmentExpression(Java8Parser.AssignmentExpressionContext ctx) {
        // assignmentExpression
        //	:	conditionalExpression
        //	|	assignment
        //	;
        if(isIteration > 0){
            // we can't get the token by api, so we use dfs method
            if(ctx.conditionalExpression()!=null){
                dfsNode(ctx.conditionalExpression(),102);
                // find multi-action pattern
                super.visitConditionalExpression(ctx.conditionalExpression());
            }else{
                // TODO: deal with assignment
                visitAssignment(ctx.assignment());
            }
        }else{
            // find multi-action pattern
            super.visitAssignmentExpression(ctx);
        }
        return 0;
    }

    // deal with left and right RDD in assignment
    @Override
    public Object visitAssignment(Java8Parser.AssignmentContext ctx) {
        // assignment
        //	:	leftHandSide assignmentOperator expression
        //	;
        if(isIteration>0){
            // TODO: null point error
            // w[i]=2*rand.nextDouble()-1;
            if(ctx.leftHandSide().expressionName()!=null){
                addLeft(isIteration,ctx.leftHandSide().expressionName().start);
            }
            visitExpression(ctx.expression());
        }
        return 0;
    }

    // deal with lambda expression right away from the '='
    @Override
    public Object visitLambdaExpression(Java8Parser.LambdaExpressionContext ctx) {
        // lambdaExpression
        //	:	lambdaParameters '->' lambdaBody
            // lambdaBody
            //	:	expression
            //	|	block
            //	;
        if(isIteration>0){
            Java8Parser.LambdaBodyContext lambdaBody = ctx.lambdaBody();
            // TODO: didn't consider about lambdaBody's for statement, NOW it's ok.
            if(lambdaBody.expression()!=null){
                visitExpression(lambdaBody.expression());
            }else{
                visitBlockStatements(lambdaBody.block().blockStatements());
            }
        }
        return 0;
    }

    // deal with BlockStatements in for statement
    @Override
    public Object visitBlockStatements(Java8Parser.BlockStatementsContext ctx) {
        if(isIteration>0){
            for(int i=0;i<ctx.getChildCount();i++){
                Java8Parser.BlockStatementContext block = (Java8Parser.BlockStatementContext) ctx.getChild(i);
                if(block.localVariableDeclarationStatement()!=null){
                    // ok, let's think about how to organize visit local variable declaration
                    // => It's ok
                    visitLocalVariableDeclaration(block.localVariableDeclarationStatement().localVariableDeclaration());
                }else if(block.classDeclaration()!=null){
                    // we don't need to think about it(for iteration and class declaration????) : no
                    // TODO: deal with class declaration in for statement
                    super.visitClassDeclaration(block.classDeclaration());
                }else if(block.statement()!=null){
                    visitStatement(block.statement());
                }
            }
        }else{
            super.visitBlockStatements(ctx);
        }
        return 0;
    }

    // deal with the statement in for statement
    @Override
    public Object visitStatement(Java8Parser.StatementContext ctx) {
        if(isIteration>0){
            if(ctx.statementWithoutTrailingSubstatement()!=null){
                visitStatementWithoutTrailingSubstatement(ctx.statementWithoutTrailingSubstatement());
                //	|	breakStatement : no
                //	|	continueStatement :no
                //	|	returnStatement :no
                //	|	synchronizedStatement :no
                //	|	throwStatement : no
                //	|	tryStatement : no
            }else if(ctx.ifThenStatement()!=null){
                // ifThenStatement :star
                    //	:	'if' '(' expression ')' statement
                    //	;
                // TODO: NULL pointer error
                // if (from != to) {
                //        edges.add(e);
                //      }
                if(ctx.ifThenStatement().statement()!=null){
                    visitStatement(ctx.ifThenStatement().statement());
                }
            }else if(ctx.ifThenElseStatement()!=null){
                // ifThenElseStatement :star
                    //	:	'if' '(' expression ')' statementNoShortIf 'else' statement
                    //	;
                visitStatementNoShortIf(ctx.ifThenElseStatement().statementNoShortIf());
                visitStatement(ctx.ifThenElseStatement().statement());
            }else if(ctx.whileStatement()!=null){
                // whileStatement: ok
                visitWhileStatement(ctx.whileStatement());
            }else if(ctx.forStatement()!=null){
                // forStatement: ok
                visitForStatement(ctx.forStatement());
            }
        }else{
            super.visitStatement(ctx);
        }
        return 0;
    }

    // deal with statement without trailing sub statement
    @Override
    public Object visitStatementWithoutTrailingSubstatement(Java8Parser.StatementWithoutTrailingSubstatementContext ctx) {
        if(isIteration>0){
            // statementWithoutTrailingSubstatement :star
            // block : no
            //	|	emptyStatement : no
            //	|	expressionStatement: star
                // 	:	assignment : star
                //	|	preIncrementExpression :no
                //	|	preDecrementExpression : no
                //	|	postIncrementExpression : no
                //	|	postDecrementExpression : no
                //	|	methodInvocation : star
                //	|	classInstanceCreationExpression: no
            //	;
            if(ctx.expressionStatement()!=null){
                // deal with assignment and methodInvocation
                Java8Parser.StatementExpressionContext statementExpression =
                        ctx.expressionStatement().statementExpression();
                if(statementExpression.assignment()!=null){
                    visitAssignment(statementExpression.assignment());
                }else if(statementExpression.methodInvocation()!=null){
                    // methodInvocation
                    //	:	methodName '(' argumentList? ')'
                    //	|	typeName '.' typeArguments? Identifier '(' argumentList? ')'
                    //	|	expressionName '.' typeArguments? Identifier '(' argumentList? ')'
                    //	|	primary '.' typeArguments? Identifier '(' argumentList? ')'
                    //	|	'super' '.' typeArguments? Identifier '(' argumentList? ')'
                    //	|	typeName '.' 'super' '.' typeArguments? Identifier '(' argumentList? ')'
                    //	;
                    // presently do nothing :star
                    // TODO: deal with methodInvocation in StatementWithoutTrailing sub statement
                    super.visitMethodInvocation(statementExpression.methodInvocation());
                }
            }
            else if(ctx.block()!=null){
                // this is only for block in for statement
                super.visitBlockStatements(ctx.block().blockStatements());
            }
            //	|	assertStatement :no
            //	|	switchStatement :star
            else if(ctx.switchStatement()!=null){
                List<Java8Parser.SwitchBlockStatementGroupContext> groups =
                        ctx.switchStatement().switchBlock().switchBlockStatementGroup();
                for(int j = 0; j < groups.size(); j++){
                    visitBlockStatements(groups.get(j).blockStatements());
                }
            }
            //	|	doStatement :star
            //	:	'do' statement 'while' '(' expression ')' ';'
            //	;
            else if(ctx.doStatement()!=null){
                visitStatement(ctx.doStatement().statement());
            }
        }else{
            super.visitStatementWithoutTrailingSubstatement(ctx);
        }
        return 0;
    }

    // deal with statement with no short if in for statement
    @Override
    public Object visitStatementNoShortIf(Java8Parser.StatementNoShortIfContext ctx) {
        if(isIteration>0){
            // statementNoShortIf
            //	:	statementWithoutTrailingSubstatement
            //	|	labeledStatementNoShortIf
            //	|	ifThenElseStatementNoShortIf
            //	|	whileStatementNoShortIf
            //	|	forStatementNoShortIf
            //	;
            if(ctx.statementWithoutTrailingSubstatement()!=null){
                visitStatementWithoutTrailingSubstatement(ctx.statementWithoutTrailingSubstatement());
            }else if(ctx.labeledStatementNoShortIf()!=null){
                // TODO : presently do nothing with labeled statement with no short if
                super.visitLabeledStatementNoShortIf(ctx.labeledStatementNoShortIf());
            }else if(ctx.ifThenElseStatementNoShortIf()!=null){
                // ifThenElseStatementNoShortIf
                //	:	'if' '(' expression ')' statementNoShortIf 'else' statementNoShortIf
                //	;
                List<Java8Parser.StatementNoShortIfContext> statementsNSI =
                        ctx.ifThenElseStatementNoShortIf().statementNoShortIf();
                visitStatementNoShortIf(statementsNSI.get(0));
                visitStatementNoShortIf(statementsNSI.get(1));
            }else if(ctx.whileStatementNoShortIf()!=null){
                // whileStatementNoShortIf
                //	:	'while' '(' expression ')' statementNoShortIf
                //	;
                visitStatementNoShortIf(ctx.whileStatementNoShortIf().statementNoShortIf());
            }else if(ctx.forStatementNoShortIf()!=null){
                // forStatementNoShortIf
                //	:	basicForStatementNoShortIf
                //	|	enhancedForStatementNoShortIf
                //	;
                    // basicForStatementNoShortIf
                    //	:	'for' '(' forInit? ';' expression? ';' forUpdate? ')' statementNoShortIf
                    //	;
                    // enhancedForStatementNoShortIf
                    //	:	'for' '(' variableModifier* unannType variableDeclaratorId ':' expression ')' statementNoShortIf
                    //	;
                Java8Parser.ForStatementNoShortIfContext forNSI = ctx.forStatementNoShortIf();
                if(forNSI.basicForStatementNoShortIf()!=null){
                    visit(forNSI.basicForStatementNoShortIf().statementNoShortIf());
                }else{
                    visit(forNSI.enhancedForStatementNoShortIf().statementNoShortIf());
                }
            }
        }else{
            super.visitStatementNoShortIf(ctx);
        }
        return 0;
    }

    // dfs the node to get identifier
    private void dfsNode(ParserRuleContext node,int type){
        if(node.getTokens(type).size()>0&&
                node.getChild(0).getChildCount()==0&&
                (Java8Parser.TypeNameContext.class == node.getClass()||
                        Java8Parser.ExpressionNameContext.class == node.getClass())){
            // save variables declared before
            Token var = node.getTokens(102).get(0).getSymbol();
            for(String key : identifiers.keySet()){
                Declaration declare = identifiers.get(key);
                if(declare.identifier.equals(var.getText())){
                    addRight(isIteration,var);
                }
            }
        }
        for(int i=0;i<node.getChildCount();i++){
            if(node.getChild(i).getChildCount()>0){
                dfsNode((ParserRuleContext) node.getChild(i),type);
            }
        }
    }

    // add left candidate RDD
    private void addLeft(int iterationNum,Token id){
        if(!isDuplicate(left.get(iterationNum-1),id)){
            left.get(iterationNum-1).add(id);
        }
    }

    // add right candidate RDD
    private void addRight(int iterationNum,Token id){
        if(!isDuplicate(right.get(iterationNum-1),id)){
            right.get(iterationNum-1).add(id);
        }
    }

    // judge whether there is duplication in left and right RDDs
    private boolean isDuplicate(List<Token> tokens, Token t){
        for(Token token : tokens){
            if(token.getText().equals(t.getText())){
                return true;
            }
        }
        return false;
    }

    // filter right RDD
    private List<Token> filterRight(int iterationNum){
        // use token, for the coming future
        List<Token> res = new ArrayList<Token>();
        List<Token> curRight = right.get(iterationNum-1);
        List<Token> curLeft = left.get(iterationNum-1);
        for(Token t: curRight){
            if(!isDuplicate(curLeft,t)){
                res.add(t);
            }
        }
        return res;
    }

    // Step 3. find multi-action pattern
    // iteration + action = normal action(actually we don't consider about declaration in iteration)
    @Override
    public Object visitMethodInvocation(Java8Parser.MethodInvocationContext ctx) {
        // methodInvocation
        //	:	methodName '(' argumentList? ')'
        //	|	typeName '.' typeArguments? Identifier '(' argumentList? ')'
        //	|	expressionName '.' typeArguments? Identifier '(' argumentList? ')'
        //	|	primary '.' typeArguments? Identifier '(' argumentList? ')'
        //	|	'super' '.' typeArguments? Identifier '(' argumentList? ')'
        //	|	typeName '.' 'super' '.' typeArguments? Identifier '(' argumentList? ')'
        //System.out.println(ctx.getText());
        if(needToJudgeAction(ctx.getText())){
            char first = ctx.getText().charAt(0);
            if(first!='.'){
                curRDD = dfsNodeToFindRDD(ctx,102);
            }
            if(ctx.methodName()!=null){
                judgeAction(ctx.methodName().getText());
            }else if(ctx.Identifier()!=null){
                judgeAction(ctx.Identifier().getText());
            }
            if(Pattern.matches(CacheUtils.cachePattern,ctx.getText())
            &&identifiers.containsKey(curRDD.getText())){
                Declaration declaration = identifiers.get(curRDD.getText());
                declaration.cacheLine = ctx.stop.getLine();
                declaration.cached = true;
            }
        }
        if(ctx.argumentList()!=null){
            super.visit(ctx.argumentList());
        }
        return 0;
    }
    @Override
    public Object visitMethodInvocation_lf_primary(Java8Parser.MethodInvocation_lf_primaryContext ctx) {
        // methodInvocation_lf_primary
        //	:	'.' typeArguments? Identifier '(' argumentList? ')'
        //	;
        //System.out.println(ctx.getText());
        if(needToJudgeAction(ctx.getText())){
            judgeAction(ctx.Identifier().getText());
        }
        if(ctx.argumentList()!=null){
            super.visit(ctx.argumentList());
        }
        return 0;
    }

    @Override
    public Object visitMethodInvocation_lfno_primary(Java8Parser.MethodInvocation_lfno_primaryContext ctx) {
        //methodInvocation_lfno_primary
        //	:	methodName '(' argumentList? ')'
        //	|	typeName '.' typeArguments? Identifier '(' argumentList? ')'
        //	|	expressionName '.' typeArguments? Identifier '(' argumentList? ')'
        //	|	'super' '.' typeArguments? Identifier '(' argumentList? ')'
        //	|	typeName '.' 'super' '.' typeArguments? Identifier '(' argumentList? ')'
        //	;
        //System.out.println(ctx.getText());
        if(needToJudgeAction(ctx.getText())){
            char first = ctx.getText().charAt(0);
            if(first!='.'){
                curRDD = dfsNodeToFindRDD(ctx,102);
            }
            if(ctx.methodName()!=null){
                judgeAction(ctx.methodName().getText());
            }else if(ctx.Identifier()!=null){
                judgeAction(ctx.Identifier().getText());
            }
        }
        if(ctx.argumentList()!=null){
            super.visit(ctx.argumentList());
        }
        return 0;
    }

    // dfs to find the rdd name
    private Token dfsNodeToFindRDD(ParserRuleContext node,int type){
        if(node.getTokens(type).size()>0&&
                node.getChild(0).getChildCount()==0&&
                (Java8Parser.TypeNameContext.class == node.getClass()||
                        Java8Parser.ExpressionNameContext.class == node.getClass())){
            // save variables declared before
            return node.getTokens(102).get(0).getSymbol();
        }
        for(int i=0;i<node.getChildCount();i++){
            if(node.getChild(i).getChildCount()>0){
                return dfsNodeToFindRDD((ParserRuleContext) node.getChild(i),type);
            }
        }
        return null;
    }
    
    // add RDD's invoking action
    private void addRDDActionAppearance(RDDCallAction rddCallAction){
        String rddName = rddCallAction.rdd.get(0).getText();
        if(rddAppearance.containsKey(rddName)){
            RDDCallAction curRDDCallAction = rddAppearance.get(rddName);
            curRDDCallAction.rdd.addAll(rddCallAction.rdd);
            curRDDCallAction.times++;
        }else{
            rddAppearance.put(rddName,rddCallAction);
        }
    }

    // whether the method need to be determine whether it is action
    private boolean needToJudgeAction(String line){
        return line.contains(".");
    }

    // to judge action
    private void judgeAction(String methodName){
        if(Pattern.matches(CacheUtils.actionPattern,methodName)){
            // is action
            List<Token> rdd = new ArrayList<Token>();
            rdd.add(curRDD);
            addRDDActionAppearance(new RDDCallAction(rdd,1));
        }
    }

    // to add rdd gained from step 3 in toCache
    public void addMultiActionRDD(){
       for(String key: rddAppearance.keySet()){
           RDDCallAction rca = rddAppearance.get(key);
           if(rca.times>=2
                   &&(!identifiers.get(rca.rdd.get(0).getText()).cached||
                   identifiers.get(rca.rdd.get(0).getText()).cacheLine>rca.rdd.get(0).getLine())){
               // TODO: add which one???, the first one is the earliest one, it's ok currently
               toCache.add(new CacheAdvice(rca.rdd.get(0),rca.rdd.get(0).getLine(), CacheUtils.MULTIACTION));
           }
       }
    }

    // list all actions:
    /*
    https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/JavaRDD.html
    1. aggregate
        treeAggregate
    2. collect,
        collectAsync
        collectPartitions
    3. count,
        countApprox
        countApproxDistinct
        countAsync
        countByKey
        countByValue
        countByValueApprox
    4. first,
    5. fold
    6. foreach,
        foreachAsync
        foreachPartition
        foreachPartitionAsync
    7. max
    8. min
    9. reduce,
        treeReduce
    10. saveAsObjectFile,
        saveAsTextFile,
        saveAsSequenceFile,
    11. take,
        takeAsync
        takeOrdered,
        takeSample,
    12. top
     */
}

// RDD declaration
class Declaration{
    public String type;
    public String identifier;
    public int line;
    public boolean cached;
    public int cacheLine;
    public Declaration(String type, String identifier, int line, boolean cached, int cacheLine) {
        this.type = type;
        this.identifier = identifier;
        this.line = line;
        this.cached = cached;
        this.cacheLine = cacheLine;
    }
}

// the times of RDD calling action
class RDDCallAction {
    public List<Token> rdd; // the lower line, the higher priority, record all lines
    public int times; // how many times

    public RDDCallAction(List<Token> rdd, int times) {
        this.rdd = rdd;
        this.times = times;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(Token t:rdd){
            sb.append("<").append(t.getText()).append(", ").append(t.getLine()).append(">\n");
        }
        return sb.toString();
    }
}

// cache advice
class CacheAdvice{
    public Token reason;
    public int adviseLine;
    public String type;
    public CacheAdvice(Token reason, int adviseLine, String type) {
        this.reason = reason;
        this.adviseLine = adviseLine;
        this.type = type;
    }
}