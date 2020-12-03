package java8;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;
import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CacheUtils {
    public static String ITERATION = "Iteration";
    public static String MULTIACTION = "MultiAction";
    public static String RDDPATTERN = "(Java)(Double|Hadoop|NewHadoop|Pair)?(RDD)(<.*>)?";
    public static String actionPattern = "(((a|treeA)(ggregate))|((collect)(Async|Partitions)?)|((count)(Approx|ApproxDistinct|Async|ByKey|ByValue|ByValueApprox)?)|(first)|(fold)|((foreach)(Partition)?(Async)?)|(max)|(min)|((treeR|r)(educe))|((saveAs)(Object|Text|Sequence)(File))|((take)(Async|Ordered|Sample)?)|(top))";
    public static String cachePattern = ".*((cache\\(.*\\))|(persist\\(.*\\))).*";
    public void runAntlr(String fileName) throws Exception{
        // 对每一个输入的字符串，构造一个 ANTLRStringStream 流 input
        //ANTLRInputStream input = new ANTLRInputStream(new FileInputStream(fileName));
        CharStream input = CharStreams.fromFileName(fileName);
        // 用 in 构造词法分析器 lexer，词法分析的作用是将字符聚集成单词或者符号
        Java8Lexer lexer = new Java8Lexer(input);
        // 用词法分析器 lexer 构造一个记号流 tokens
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        // 再使用 tokens 构造语法分析器 parser,至此已经完成词法分析和语法分析的准备工作
        Java8Parser parser = new Java8Parser(tokens);
        // 最终调用语法分析器的规则 r（这个是我们在g4里面定义的那个规则），完成对表达式的验证
        Java8Parser.CompilationUnitContext tree = parser.compilationUnit();
        // 使用Listener进行遍历
//        MyListener listener = new MyListener();
//        ParseTreeWalker.DEFAULT.walk(listener,tree);
//        System.out.println("generating results done!");
        // 使用Visitor进行遍历
        MyVisitor visitor = new MyVisitor();
        String interval = "\\\\";
        if(fileName.contains("/")){
            interval = "/";
        }
        visitor.curClass = fileName.split(interval)[fileName.split(interval).length-1].split("\\.")[0];
        visitor.visit(tree);

        visitor.addMultiActionRDD();
        // test Work
         testWork(visitor);
        // add our code into source code
        updateCode(fileName,convertToCache(visitor),fileName+".mx");
    }
    // test whether our FindCacheCandidateVisitor is worked
    public void runAntlrNewVisitor(String fileName) throws Exception{
        // TODO: add test function
        // 对每一个输入的字符串，构造一个 ANTLRStringStream 流 input
        //ANTLRInputStream input = new ANTLRInputStream(new FileInputStream(fileName));
        CharStream input = CharStreams.fromFileName(fileName);
        // 用 in 构造词法分析器 lexer，词法分析的作用是将字符聚集成单词或者符号
        Java8Lexer lexer = new Java8Lexer(input);
        // 用词法分析器 lexer 构造一个记号流 tokens
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        // 再使用 tokens 构造语法分析器 parser,至此已经完成词法分析和语法分析的准备工作
        Java8Parser parser = new Java8Parser(tokens);
        // 最终调用语法分析器的规则 r（这个是我们在g4里面定义的那个规则），完成对表达式的验证
        Java8Parser.CompilationUnitContext tree = parser.compilationUnit();
        // 使用Visitor进行遍历
        FindCacheCandidateVisitor fccVisitor = new FindCacheCandidateVisitor();
        fccVisitor.visitCompilationUnit(tree);
        List<RDD> toBeCache = fccVisitor.findToBeCache();
        // TODO: add these tests into functions
        Map<String,List<RDD>> allRdds = fccVisitor.rdds;
        for(String key :allRdds.keySet()){
            for(RDD r:allRdds.get(key)){
                System.out.print(r+" ");
            }
            System.out.println();
        }
        System.out.println("==================***===================");
        for(RDD r:toBeCache){
            System.out.println(r.toString());
        }
    }
    // test whether the work is ok
    private void testWork(MyVisitor visitor){
        // 检验 step1
        for(String key: visitor.identifiers.keySet()){
            Declaration d = visitor.identifiers.get(key);
            System.out.println(d.type+" "+d.identifier+" "+d.line+" "+d.cached+" cacheLine: "+d.cacheLine);
        }
        // 检验 step2的一部分
        for(List<Token> ts:visitor.right){
            System.out.println("======right");
            for(Token t:ts){
                System.out.println(t.getText());
            }
        }
        for(List<Token> ts:visitor.left){
            System.out.println("======left");
            for(Token t:ts){
                System.out.println(t.getText());
            }
        }
        // test step3
        System.out.println("================multiActon");
        for(String key: visitor.rddAppearance.keySet()){
            System.out.println(visitor.rddAppearance.get(key));
        }
        // print toCache
        System.out.println("================toCache=================");
        for(CacheAdvice s:visitor.toCache){
            System.out.printf("<%s:%s, advisedLine: %s, cacheType: %s>\n",s.reason.getText(),
                    s.reason.getLine(),s.adviseLine,s.type);
        }
    }

    // convert toCache into <modifiedLine, item in toCache>
    private Map<Integer,CacheAdvice> convertToCache(MyVisitor visitor){
        Map<Integer,CacheAdvice> map = new HashMap<Integer, CacheAdvice>();
        for(CacheAdvice s:visitor.toCache){
            if(s.type.equals(ITERATION)){
                // from start line, eat a sentence => to the foot of sentence
                map.put(visitor.identifiers.get(s.reason.getText()).line,s);
            }else if(s.type.equals(MULTIACTION)){
                // from the line before s's advisedLine
                map.put(s.adviseLine,s);
            }
        }
        return map;
    }

    // method to update Code
    private void updateCode(String fileName, Map<Integer,CacheAdvice> map, String newName) throws Exception {
        writeFile(newName,readFileContent(fileName,map));
    }

    // read file and record all contents
    private StringBuilder readFileContent(String fileName, Map<Integer,CacheAdvice> map) throws Exception {
        StringBuilder sb = new StringBuilder();
        int cur = 0;
        String line = null;
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        while((line = br.readLine())!=null){
            cur++;
            if(map.get(cur)!=null){
                CacheAdvice cache = map.get(cur);
                if(cache.type.equals(ITERATION)){
//                    if(!line.contains(";")){
//                        sb.append(line).append("\n");
//                        while ((line = br.readLine())!=null){
//                            cur++;
//                            if(line.contains(";")){
//                                break;
//                            }
//                            sb.append(line).append("\n");
//                        }
//                    }
                    int index = line.indexOf(';');
                    sb.append(line, 0, index).append(".cache()").append(line,index,line.length()).append("\n");
                }else{
                    // TODO: add more cache type
                    for(char ch:line.toCharArray()){
                        if(ch==9 || ch==32){
                            sb.append(ch);
                        }else{
                            break;
                        }
                    }
                    sb.append(cache.reason.getText()).append(".cache();\n");
                    sb.append(line).append("\n");
                }
            }else{
                sb.append(line).append("\n");
            }
        }
        br.close();
        return sb;
    }

    // write file
    private void writeFile(String fileName,StringBuilder sb) throws Exception {
        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
        bw.write(sb.toString());
        bw.close();
    }


    public static void main(String[] args) throws Exception{
//        for(String fileName :args){
//            System.out.println(fileName);
//            run(fileName);
//        }
        // test list
//        List<Declaration> ls = new ArrayList<Declaration>();
//        ls.add(new Declaration("11","22",3,false,1));
//        Declaration d = ls.get(0);
//        d.type="22";
//        System.out.println(ls.get(0).type);
//        ls.set(0,d);
//        System.out.println(ls.get(0).type);

        // test action pattern
//        String test = "aggregate\n" +
//                "        treeAggregate\n" +
//                "    collect\n" +
//                "        collectAsync\n" +
//                "        collectPartitions\n" +
//                "    count\n" +
//                "        countApprox\n" +
//                "        countApproxDistinct\n" +
//                "        countAsync\n" +
//                "        countByKey\n" +
//                "        countByValue\n" +
//                "        countByValueApprox\n" +
//                "    first\n" +
//                "    fold\n" +
//                "    foreach\n" +
//                "        foreachAsync\n" +
//                "        foreachPartition\n" +
//                "        foreachPartitionAsync\n" +
//                "    max\n" +
//                "    min\n" +
//                "    reduce\n" +
//                "        treeReduce\n" +
//                "    saveAsObjectFile\n" +
//                "        saveAsTextFile\n" +
//                "        saveAsSequenceFile\n" +
//                "    take\n" +
//                "        takeAsync\n" +
//                "        takeOrdered\n" +
//                "        takeSample\n" +
//                "    top\n" +
//                "jfioawedjfowjeg\n" +
//                "fjow2ejgioreadmfo\n" +
//                "flatmap\n" +
//                "maptopair\n";
//        String[] tests = test.split("\\s+");
//        for(String s: tests){
//            if(!Pattern.matches(actionPattern,s)){
//                System.out.println(s+": ");
//            }
//        }
//        System.out.println(Class.forName("org.apache.spark.api.java.JavaRDDLike")
//                .isAssignableFrom(Class.forName("org.apache.spark.api.java.JavaDoubleRDD")));
//        System.out.println(Class.forName("org.apache.spark.api.java.JavaRDDLike")
//                .isAssignableFrom(Class.forName("org.apache.spark.api.java.JavaRDD")));
    }
}
