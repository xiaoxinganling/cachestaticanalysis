package java8;

import java.io.File;

public class Main {
    public static void main(String[] args) throws Exception{
        File f = new File(args[0]);
        File[] fs = f.listFiles();
        for(File file :fs){
            if(!file.isDirectory()){
                String fileName = file.getAbsolutePath();
                System.out.println(fileName);
                if(fileName.equals("D:\\IDEAWorkspace\\CacheStaticAnalysis\\src\\main\\java\\java8\\test\\JavaPageRank.txt")){
                    new CacheUtils().runAntlr(fileName);
                }
            }
        }
    }
}
