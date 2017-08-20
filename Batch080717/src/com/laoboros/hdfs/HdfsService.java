package com.laoboros.hdfs;

import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
		conf.set("fs.defaultFS","hdfs://localhost:8020");
		
		//step-3:ToolRunner.run -- GenericOptionParser Parsing the command line arguments, identify the environment setting and input arguments
		//This environment setting will set in the configuration 
		//Remaining arguments will pass to run method.
		
		try {
			int i=ToolRunner.run(conf, new HdfsService(), args);
			if(i==0)
			{
				System.out.println("SUCCESS");
			}
		} catch (Exception e) {
			System.out.println("FAILED");
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		
		//step-1: Get the configuration
		Configuration conf = super.getConf();
		
		//Step: 2 Create FileSystem object
		
		FileSystem hdfs = FileSystem.get(conf);//fs.defaultFS
		
		//step:3 : Creating a file = Creating Metadata + Add Data
		
		//Creating MetaData = create an empty file
			//EmptyFile = hdfsdestinationdir(/user/edureka)+onlyFileName(WordCount.txt) and convert to Path 
		
		//Get the FileName
		final String edgeNodeLocalFileName = args[0]; //user/edureka/WordCount.txt  --> WordCount.txt
		//Get the destination directory from command line
		
		final String destinationDir = args[1];
		//Convert it to Path
		
//		/WordCount.txt
		
		Path hdfsFileName = new Path(destinationDir,edgeNodeLocalFileName); //make sure edgenode local filename should not start with /
		
		FSDataOutputStream fsdos=hdfs.create(hdfsFileName);
		
		//step-4: Add data
		//Split into Blocks
		
		//Read the file
		
		InputStream is = new FileInputStream(edgeNodeLocalFileName);
		
		//Identification of Datanode to DataBlocks -- -BlockplacementPolicy
		//Write the file
		//Meet the replication
		//Sync Metadata
		//Handling failures : This is taken care by hdfs by default.
		IOUtils.copyBytes(is, fsdos, conf, Boolean.TRUE);
		return 0;
	}

	

}
