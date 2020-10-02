package java8;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.io.File;
import java.io.FileInputStream;

public class Main {
    public static void run(String fileName) throws Exception{
        // 对每一个输入的字符串，构造一个 ANTLRStringStream 流 in
        ANTLRInputStream input = new ANTLRInputStream(new FileInputStream(fileName));
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
                System.out.println(file.getAbsolutePath());
                run(file.getAbsolutePath());
            }
        }
    }
}
