package util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class util {
	public static void out(String str){
		System.out.println(str);
	}
	
	public static void out(int num){
		System.out.println(num);
	}
	
	public static long checkSize(String file) throws IOException, URISyntaxException{
		 Configuration conf = new Configuration(); 
		 FileSystem fileFS = FileSystem.get(new URI(file) ,conf); 
		 FileStatus fileStatus = fileFS.getFileStatus(new Path(file)); 
		 
		 return fileStatus.getLen();
	}
}
