package Genetic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import util.util;

public class GeneticI extends Configured implements Tool{
	public static class ProMapper extends Mapper<Object,Text,Text,Text>{

		//int Group=5000;
		int N=99;
		double P=0.8;
		double B=0.1;
		double[][] dis=new double[N+2][N+2];
		int cnt=0,cnt2=0;
		Vector<String> tmp=new Vector<String>();
		pos[] Map=new pos[N+2];
		
		public class pos{
			int no;
			int x,y;
			
			public pos(int no,int x,int y){
				this.no=no;
				this.x=x;
				this.y=y;
			}
		}
		
		int getPos(int[] path,int p){
			for(int i=0;i<path.length;i++){
				if(path[i]==p){
					return i;
				}
			}
			return -1;
		}
		
		void varation(int[] a){  
		    int temp;  
		    //变异的数量，即，群体中的个体以PM的概率变异,变异概率不宜太大  
		  
		      //确定发生变异的位  
		      int i = (int) (Math.random() * N);  
		      int j = (int) (Math.random() *N);  
		  
		     //exchange  
		     temp  = a[i];  
		     a[i] = a[j];   
		     a[j] = temp;  
		  	   
		//    return a;
		}  
		
		void reverse(int[] a,int s,int e){
			int i=s,j=e;
			while(i<j){
				int tmp=a[i];
				a[i]=a[j];
				a[j]=tmp;	
				i++;j--;
			}
			//return a;
		}
		
		void rotate(int[] a,int s,int e,int p){
			reverse(a,s,p-1);
			reverse(a,p,e-1);
			reverse(a,s,e-1);
			//return a;
		}
		void show(int[] t){
			for(int i=0;i<t.length;i++){
				System.out.print(t[i]+" ");
			}System.out.println();
		}
		void cout(String a){
			System.out.println(a);
		}
		
		int calSumDis(int[] a){
			int sum=0;
			for(int i=1;i<a.length;i++){
				sum+=dis[a[i-1]][a[i]];
			}
			return sum;
		}

		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			Path[] caches=DistributedCache.getLocalCacheFiles(context.getConfiguration());
			if(caches==null||caches.length<=0){
				System.exit(1);
				} 

			BufferedReader br=new BufferedReader(new FileReader(caches[0].toString()));
			String line ;
			int[] p=new int[3];
			int agxcul=0;
	        while((line=br.readLine())!=null){
	        	StringTokenizer itr=new StringTokenizer(line);
	        	
	        	int i=0;
	        	while(itr.hasMoreTokens()){
	        		p[i]=Integer.parseInt(itr.nextToken());i++;
	        	}
	        	Map[agxcul]=new pos(p[0],p[1],p[2]);
	        	agxcul++;
	        }
	        
	        
	        for(int i=0;i<N;i++){
	        	for(int j=i+1;j<N;j++){
	        		dis[i][j]=dis[j][i]=Math.sqrt(  Math.pow(  (Map[i].x-Map[j].x)   , 2) +
	        				Math.pow( (Map[i].y-Map[j].y)   , 2)) ;
	        	}
	        }
		}
		
		
		@Override
		public void map(Object key,Text value,Context context)throws IOException,InterruptedException{

			if(cnt<=2500){cnt2=cnt;
				cnt++;
				tmp.add(value.toString());
				return ;
			}else{
				if(cnt2<=0){
					return ;
				}
				String a=tmp.get(cnt2);cnt2--;
				tmp.remove(0);
				String b=value.toString();
				//cout("=========================================");
				String[] p=a.split("\t")[1].split(" ");
				String[] q=b.split("\t")[1].split(" ");
				int[] path1=new int[N];
				int[] path2=new int[N];
				int[] res=new int[N];
				for(int i=0;i<N;i++){
					path1[i]=Integer.parseInt(p[i]);
					path2[i]=Integer.parseInt(q[i]);
				}
				//cout("path1's lentgh : "+calSumDis(path1));
				//cout("path2's length : "+calSumDis(path2));
				int start=dis[ path1[0] ][ path1[1] ]<dis[ path2[0] ][ path2[1] ]?path1[0]:path2[0];
				res[0]=start;
				int first=getPos(path1,start);
				int second=getPos(path2,start);
				rotate(path1,0,N,first);
				rotate(path2,0,N,second);
				for(int i=1;i<N;i++){
					//show(path1);
					//show(path2);
					//cout(path1[i-1]+"->"+path1[i]+" | dis : "+dis[path1[i-1]][path1[i]] );
					//cout(path2[i-1]+"->"+path2[i]+" | dis : "+dis[path2[i-1]][path2[i]] );
					
					if(dis[path1[i-1]][path1[i]]<dis[path2[i-1]][path2[i]] ){
						res[i]=path1[i];
						int pos=getPos(path2,path1[i]);
						rotate(path2,i,N,pos);
					}else{
						res[i]=path2[i];
						int pos=getPos(path1,path2[i]);
						rotate(path1,i,N,pos);
					}
					//show(res);cout("================================");
					
					//THGA
				}
				//cout("res's length : "+calSumDis(res));
				String result="";
				for(int i=0;i<N;i++){
					result+=(res[i]+" ");
				}
				context.write(new Text(calSumDis(res)+""), new Text(result));
				varation(res);
				result="";
				for(int i=0;i<N;i++){
					result+=(res[i]+" ");
				}
				context.write(new Text(calSumDis(res)+""), new Text(result));
			}
		}
	}
	
	public static class ProReducer extends Reducer<Text,Text,Text,Text>{
		
		double P=0.6;
		int Group=5000;
		int num=(int) (Group*P);
		int Fnum=(int)(Group*(1-P));
		int cnt=0;
		int cnt2=0;
		HashMap<String,String> S=new HashMap<String,String>();
		int max=100000;
		String mark;
		
		@Override
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
			for(Text v : values){
				if(Integer.parseInt(key.toString())<max){
					max=Integer.parseInt(key.toString());
					mark=v.toString();
				}
				if(cnt>=num){
					for(int i=0;i<cnt2;i++){
						String[] tmp=S.get(i+"").split("\t");
						context.write(new Text(tmp[0]), new Text(tmp[1]));
					}					
					S=new HashMap<String,String>();
					cnt2=0;
				}else{
					if(cnt2<Fnum){
						S.put(cnt2+"", key.toString()+"\t"+v.toString());cnt2++;
					}
					cnt++;
					context.write(new Text(key.toString()), new Text(v.toString()));
				}
			}
			
			context.write(new Text(max+""),new Text(mark) );
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf=getConf();
		DistributedCache.addCacheFile(new URI("source"),conf);
		Job job=new Job(conf,"GeneticI");
		job.setJarByClass(GeneticI.class);
		job.setMapperClass(ProMapper.class);
		job.setReducerClass(ProReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
