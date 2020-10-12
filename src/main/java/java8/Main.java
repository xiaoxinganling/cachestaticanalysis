package java8;

import scala.Int;

import java.io.File;

public class Main {
    public static void main(String[] args) throws Exception{
        long[][] time = new long[3][3];
        int cur = 0;
        File f = new File(args[0]);
        File[] fs = f.listFiles();
        for(File file :fs){
            if(!file.isDirectory()){
                String fileName = file.getAbsolutePath();
                //System.out.println(fileName);
                if(fileName.equals("D:\\IDEAWorkspace\\CacheStaticAnalysis\\src\\main\\java\\java8\\test\\JavaPageRank.txt")
                || fileName.equals("D:\\IDEAWorkspace\\CacheStaticAnalysis\\src\\main\\java\\java8\\test\\JavaHdfsLR.txt")
                || fileName.equals("D:\\IDEAWorkspace\\CacheStaticAnalysis\\src\\main\\java\\java8\\test\\JavaTC.txt")
                //|| fileName.equals("D:\\IDEAWorkspace\\CacheStaticAnalysis\\src\\main\\java\\java8\\test\\JavaKMeans.txt")
                //|| fileName.equals("D:\\IDEAWorkspace\\CacheStaticAnalysis\\src\\main\\java\\java8\\test\\JavaGroupByTest.txt")
                ){
                    long startTime=System.currentTimeMillis();   //start Time
                    new CacheUtils().runAntlr(fileName);
                    long endTime=System.currentTimeMillis(); //获取结束时间
                    System.out.println(fileName+": "+(endTime-startTime)+"ms");
                    // LR, PR, TC
                    time[cur/3][cur%3] = endTime-startTime;
                    cur++;
                }
            }

        }
        for(int i=0;i<3;i++){
            for(int j=0;j<3;j++){
                System.out.print(time[i][j]+" ");
            }
            System.out.println();
        }
    }
}
