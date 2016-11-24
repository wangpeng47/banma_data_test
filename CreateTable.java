package com.lkl.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;


public class CreateTable {

	public static void main(String[] args) throws Exception {
		
		
		if(args.length > 0){
			
			
			String [] array = args[0].split(",");
			
			
			if(array.length < 8){
				
				System.out.println("The number of params is not 8.");
				
				System.exit(0);
			}
			String createTableName = array[0];
			String columns[] = array[1].split(":");
			String startKey = array[2];
			String endKey = array[3];
			int numRegions = Integer.parseInt(array[4]);
			int timeToLive = Integer.parseInt(array[5]);
			String quorum = array[6];
			String clientPort = array[7];
			
		
			
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", quorum);
			conf.set("hbase.zookeeper.property.clientPort", clientPort);
			
			boolean b = ToolsUtil.createTable(createTableName, conf, columns, startKey, endKey, numRegions, timeToLive);
			if(b){
				System.out.println("CreateTable success.");
			} else {
				System.out.println("CreateTable fail.");
			}
		} else {
			System.out.println("Please enter the params of createTable.");
		}
		
		System.exit(0);
	}

}
