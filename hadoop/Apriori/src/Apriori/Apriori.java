package Apriori;

import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;

import util.*;

public class Apriori  extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		long start = System.currentTimeMillis();		
		if(args.length<3){
			util.out("jar : [input] [output] [conf]");	
			args=new String[3];
			args[0]="input.txt";
			args[1]="L";
			args[2]="conf.xml";
		}
		int res = ToolRunner.run(new Configuration(), new Apriori(), args);
		System.out.println("Use time : " + (System.currentTimeMillis() - start)/1000 + " s" );
	}

	@Override
	public int run(String[] args) throws Exception {
		String input=args[0];
		String output=args[1];
		String[] path=new String[2];
		path[0]=input;
		path[1]=output+"1";
		
		System.out.println("MapReduce...");
		int res = ToolRunner.run(getConf(),  new AprioriI(),path);

		return 0;
	}

}
