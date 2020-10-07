package java8;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

import java.io.File;
import java.util.List;
import java.util.regex.Pattern;

public class Main {
    public static String pattern = "(Java)(Double|Hadoop|NewHadoop|Pair)?(RDD)(<.*>)?";
    public static String actionPattern = "(((a|treeA)(ggregate))|((collect)(Async|Partitions)?)|((count)(Approx|ApproxDistinct|Async|ByKey|ByValue|ByValueApprox)?)|(first)|(fold)|((foreach)(Partition)?(Async)?)|(max)|(min)|((treeR|r)(educe))|((saveAs)(Object|Text|Sequence)(File))|((take)(Async|Ordered|Sample)?)|(top))";
    public static void run(String fileName) throws Exception{
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
        visitor.visit(tree);
        visitor.addMultiActionRDD();
        // 检验 step1
        for(Declaration d: visitor.identifiers){
            System.out.println(d.type+" "+d.identifier+" "+d.line);
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
        // print to cache
        System.out.println("================toCache");
        for(Token s:visitor.toCache){
            System.out.printf("<%s,%s>\n",s.getText(),s.getLine());
        }
    }
    public static void main(String[] args) throws Exception{
//        for(String fileName :args){
//            System.out.println(fileName);
//            run(fileName);
//        }
        File f = new File(args[0]);
        File[] fs = f.listFiles();
        for(File file :fs){
            if(!file.isDirectory()){
                String fileName = file.getAbsolutePath();
                System.out.println(fileName);
                if(fileName.contains("JavaPageRank")){
                    run(fileName);
                }
            }
        }
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
