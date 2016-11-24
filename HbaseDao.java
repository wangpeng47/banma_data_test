package com.lkl.hbase.lklhbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.lkl.hbase.lklhbase.HbaseBaseDao;


public class HbaseDao extends HbaseBaseDao {

	
	public List<Map<String, Object>> scanByRowKey(
			String startRow, String endRow,String tableName) {

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

		try {
			this.table = (HTable) ConnectionFactory.createConnection(
					super.getConf()).getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			Scan scan = new Scan(Bytes.toBytes(startRow), Bytes.toBytes(endRow));

			ResultScanner resultScanner = table.getScanner(scan);
			
			for (Result result : resultScanner) {
				if ((result != null) & (!result.isEmpty())) {

					Map<String, Object> map = new HashMap<String, Object>();
					String row = Bytes.toStringBinary(result.getRow());
					map.put("rowKey", row);
					
					List<Map<String,String>> keyvaluelist = new ArrayList<Map<String,String>>(); 
					
					KeyValue[] kv = result.raw();
					for(int i=0;i<kv.length;i++){
						
						String family = Bytes.toString(kv[i].getFamilyArray());
						
						String column = Bytes.toString(kv[i].getQualifierArray());
						
						String key = family+":"+column;
						
						String value = Bytes.toString(kv[i].getValueArray());
						
						Map map1 = new HashMap<String,String>();
						
						map1.put(key, value);
						keyvaluelist.add(map1);
					}
					
					map.put("rowValue",keyvaluelist);
					list.add(map);
				}
			}
			resultScanner.close();
			table.close();
			return list;
		} catch (Exception e) {

			e.printStackTrace();

			return null;
		}
	}

}
