package com.laoboros.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


//hadoop fs -put WordCount.txt /user/edureka

//java com.laboros.hdfs.HdfsService -Ddfs.replication=4 WordCount.txt /user/edureka

public class HdfsService extends Configured implements Tool 
{
	
	public static void main(String[] args) 
	{
		//step-1: Validating the input arguments
		
		if(args.length<2)
		{
			System.out.println("Java Usage HdfsService [configuration] /path/to/edgenode/input /path/to/hdfs/output/dir");
			return;
		}
		
		//step-2: Loading the configuration
		
		Configuration conf = new Configuration(Boolean.TRUE);
		
		
		//step-3:ToolRunner.run -- GenericOptionParser Parsing the command line arguments, identify the environment setting and input arguments
		//This environment setting will set in the configuration 
		//Remaining arguments will pass to run method.
		
		try {
			ToolRunner.run(conf, new HdfsService(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		
		//step-: Get the configuration
		Configuration conf = super.getConf();
		return 0;
	}

	

}
