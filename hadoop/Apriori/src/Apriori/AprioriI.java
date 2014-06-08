package Apriori;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import util.util;

public class AprioriI extends Configured implements Tool{
	public static class ProMapper extends Mapper<Object,Text,Text,Text>{
		HashSet<String> Apriori_set=new HashSet();
		public void addSubSet(Vector<String> sets){
			if(sets.size()==1){
				return ;
			}else{
				String insert="";
				for(int i=0;i<sets.size();i++){
					insert+=sets.get(i);
					Apriori_set.add(insert);					
					if(i<sets.size()-1){
						insert+=",";
					}
				}
				
				for(int i=0;i<sets.size();i++){
					Vector<String> tmp=(Vector<String>) sets.clone();
					tmp.remove(i);
					addSubSet(tmp);
				}
			}
		}
		@Override
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
			Apriori_set=new HashSet();
			Vector<String> v=new Vector<String>();
			String[] tuple=value.toString().split("\t")[1].split(",");
			for(int i=0;i<tuple.length;i++){
				Apriori_set.add(tuple[i]);
				v.add(tuple[i]);
			}
			addSubSet(v);
			Iterator<String> it=Apriori_set.iterator();
		    while(it.hasNext()){
		           String o=it.next();
		           context.write(new Text(o),new Text("1"));
		    }
		}
	}
	
	public static class ProReducer extends Reducer<Text,Text,Text,Text>{
		int min_sup;
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
				throws IOException, InterruptedException {
			context.getConfiguration().addResource(new Path("conf.xml"));
			String sup=context.getConfiguration().get("org.er.min_sup");
			if(sup==null){
				System.err.println("[ERROR]min_sup not found");
				System.exit(0);
			}
			min_sup=Integer.parseInt(sup);
		}

		@Override
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
			int sum = 0;
			for(Text n : values){
				int num=Integer.parseInt(n.toString());
				sum += num;
			}
			if(sum>=min_sup){
				context.write(key, new Text(sum+""));
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		
		Job job=new Job(conf,"AprioriI");
		job.setJarByClass(AprioriI.class);
		job.setMapperClass(ProMapper.class);
		job.setReducerClass(ProReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
