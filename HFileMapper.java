package com.lkl.hbase;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class HFileMapper extends
		Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

	final static String MAPPER_CONF_KEY = "importtsv.mapper.class";
	final static String SKIP_LINES_CONF_KEY = "importtsv.skip.bad.lines";
	final static String BULK_OUTPUT_CONF_KEY = "importtsv.bulk.output";
	final static String COLUMNS_CONF_KEY = "importtsv.columns";
	
	final static String SEPARATOR_CONF_KEY = "importtsv.separator";
	final static String TIMESTAMP_CONF_KEY = "importtsv.timestamp";
	final static String AVRO_CONF_KEY = "importtsv.avro";
	final static String ROWKEY_INDEX_CONF_KEY = "importtsv.rowkey.index";
	final static String ROWKEY_TYPE_CONF_KEY = "importtsv.rowkey.type";
	final static String DEFAULT_SEPARATOR = "\001";
	final static String ROWKEY_SEPARATOR = "_";
	final static String ROWKEY_APPEND_STR = "importtsv.rowkey.append.str";
	
	/*以特定的某一列设定导入数据的时间戳*/
	final static String TIMESTAMP_INDEX="importtsv.timestamp.index";
	
	/*设定导入时间戳的时间格式，默认gesh*/
	final static String TIMESTAMP_FORMAT="importtsv.timestamp.format";

	private String separator;
	private String rowkey_append_str;
	
	private boolean skipBadLines;
	private Counter badLineCount;
	private Configuration conf;
	private ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
	private ParserUtil parser;
	private int[] indexs;
	
	private boolean isAvro;

	public boolean getSkipBadLines() {
		return skipBadLines;
	}

	public Counter getBadLineCount() {
		return badLineCount;
	}

	public void incrementBadLineCount(int count) {
		this.badLineCount.increment(count);
	}

	@Override
	protected void setup(Context context) {
		doSetup(context);
		parser = new ParserUtil(conf.get(COLUMNS_CONF_KEY),separator);
		
		Preconditions.checkArgument(parser.getQualifierLength() == 2, "Only two column required");
		String rowkeyIndex	=	conf.get(ROWKEY_INDEX_CONF_KEY);
		rowkey_append_str = conf.get(ROWKEY_APPEND_STR);
		
		Preconditions.checkArgument(rowkeyIndex!=null, "Rowkey index required"); 
		String[] ss	=	rowkeyIndex.split(",");
		indexs	=	new int[ss.length];
		for(int i=0;i<ss.length;i++){
			indexs[i]=Integer.parseInt(ss[i]);
		}
	}

	protected void doSetup(Context context) {
		conf = context.getConfiguration();
		
		
		separator = conf.get(SEPARATOR_CONF_KEY);
		if (separator == null) {
			separator = DEFAULT_SEPARATOR;
		} else {
			separator = new String(Base64.decode(separator));
		}
		isAvro =	Boolean.parseBoolean(conf.get(AVRO_CONF_KEY));
		skipBadLines = context.getConfiguration().getBoolean(SKIP_LINES_CONF_KEY, true);
		badLineCount = context.getCounter("ImportTsv", "Bad Lines");
	}

	public void map(LongWritable offset, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		try {
			Put put = this.getPut(line,context);
			if (put != null) {
				immutableBytesWritable.set(put.getRow());
				context.write(immutableBytesWritable, put);
			}
		} catch (Exception e) {
			System.err.println("Error line : "+line);
		}
	}

	private Put getPut(String line,Context context) throws IOException, ParseException {
//		String rowkey=getRowKey(line);
		
		
		String rowkey=getMD5RowKey(line);
		if(rowkey==null){
			return null;
		}
		
		
		Put put=null;
		int index = context.getConfiguration().getInt(TIMESTAMP_INDEX, -1);
		if (-1!=index){
			
			long ts;
			try {
				String format = context.getConfiguration().get(TIMESTAMP_FORMAT);
				SimpleDateFormat sFormat=null;
				if(format!=null){
					sFormat = new SimpleDateFormat(format);
				}else{
					
					sFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				}
				
				List<String> cols = Lists.newArrayList(Splitter.on(separator).trimResults().split(line));
				ts = sFormat.parse(cols.get(index)).getTime();
				put = new Put(Bytes.toBytes(rowkey),ts);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}else{
			put=new Put(Bytes.toBytes(rowkey));
			
		}
		
		byte[] content	=	Bytes.toBytes(line);
//		if(isAvro){
//			content=AvroUtils.getAvroData(line);
//		}
		put.add(parser.getFamily(1),parser.getQualifier(1),content);
		return put;
	}
	
	
	public String getMD5RowKey(String line) {
		StringBuffer bf=new StringBuffer();
		
		List<String> cols = Lists.newArrayList(Splitter.on(separator).trimResults().split(line));
		String rowkey_pre=DigestUtils.md5Hex(cols.get(indexs[0]));
		bf.append(rowkey_pre);
		if(rowkey_append_str!=null){
			bf.append(ROWKEY_SEPARATOR).append(rowkey_append_str);
		}
		if(indexs.length>1){
			for(int i=1;i<indexs.length;i++){
				
				String str="";
				if(ToolsUtil.isDatetime(cols.get(indexs[i]))){
					try {
						str=String.valueOf(ToolsUtil.parseDatetimeToLong(cols.get(indexs[i])));
					} catch (ParseException e) {
						
						e.printStackTrace();
					}
				}else{
					str=cols.get(indexs[i]);
				}
				bf.append(ROWKEY_SEPARATOR).append(str);
			}
		}
		return bf.toString();
	}
	
	
}