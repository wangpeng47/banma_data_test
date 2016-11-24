package com.lkl.hbase;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;



public class ToolsUtil {

	public static final String datetimeFormat = "yyyy-MM-dd hh:mm:ss";

	/*
	 * @param str Datetime style:"yyyy-MM-dd hh:mm:ss", parse it to long
	 * return long;
	 */
	public static long parseDatetimeToLong(String str) throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat(datetimeFormat);
		Date startDate = df.parse(str);
		return startDate.getTime() / 1000;
	}

	public static boolean isDatetime(String str) {
		boolean isParse = true;
		SimpleDateFormat df = new SimpleDateFormat(datetimeFormat);
		try {
			df.parse(str);
		} catch (ParseException e) {
			isParse = false;
		}
		return isParse;
	}

	@SuppressWarnings("deprecation")
	public static boolean createTable(String tablename, Configuration conf,
			String[] columns, String startKey, String endKey, int numRegions,
			int timeToLive) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
		
			if (admin.tableExists(tablename)) {
				admin.close();
				return false;
			} else {
				HTableDescriptor tableDesc = new HTableDescriptor(tablename);
				for (String column : columns) {
					
					
					
					HColumnDescriptor colFamDescriptor = new HColumnDescriptor(
							column);
					colFamDescriptor.setCompressionType(Algorithm.SNAPPY);
					colFamDescriptor
							.setCompactionCompressionType(Algorithm.SNAPPY);
					colFamDescriptor.setInMemory(true);
					colFamDescriptor.setBloomFilterType(BloomType.ROWCOL);
					
					if(timeToLive>0){
						colFamDescriptor.setTimeToLive(timeToLive);
					}

					tableDesc.addFamily(colFamDescriptor);
				}
				admin.createTable(tableDesc, Bytes.toBytes(startKey), Bytes
						.toBytes(endKey), numRegions);
				admin.close();
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

}
