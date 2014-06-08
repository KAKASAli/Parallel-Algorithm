package Genetic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;

import util.*;

public class Genetic  extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		long start = System.currentTimeMillis();		
		if(args.length<3){
			util.out("jar : [input] [output] [conf]");	
			args=new String[3];
			args[0]="input";
			args[1]="G";
			args[2]="conf.xml";
		}
		int res = ToolRunner.run(new Configuration(), new Genetic(), args);
		System.out.println("Use time : " + (System.currentTimeMillis() - start)/1000 + " s" );
	}

	@Override
	public int run(String[] args) throws Exception {
		String input=args[0];
		String output=args[1];
		String[] path=new String[2];
		path[0]=input;
		path[1]=output+"1";
		FileOutputStream out=new FileOutputStream("cache");
        PrintStream p=new PrintStream(out);
		
		int GroupNum=5000;
		int GemNum=5;
		int N=99;
		for(int i=0;i<GroupNum;i++){
			List<Integer> list=new ArrayList<Integer>();
			for(int j=0;j<N;j++){
				list.add(j);
			}
			p.print("1\t");
			Collections.shuffle(list);
			for(int j=0;j<N;j++){
				p.print(list.get(j)+" ");
			}
			p.println();
		}
		System.out.println("=====data generate done!=====");
		
		Configuration conf=new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path src = new Path("cache");
        Path dst = new Path("input");
        hdfs.copyFromLocalFile(src, dst);
		
		int res = 0;
		for(int i=1;i<=GemNum;i++){
			System.out.println("MapReduce..."+i);
			res = ToolRunner.run(getConf(),  new GeneticI(),path);
			util.delDir(path[0]);
			path[0]=path[1];
			path[1]=output+(i+1);
			
		}
		return res;
	}

}
