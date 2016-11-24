package com.lkl.hbase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

public class AvroUtils {

	public static final String schemaDescription = " {    \n"
		+ " \"name\": \"hbase\", \n"
		+ " \"type\": \"record\",\n" + " \"fields\": [\n"
		+ "   {\"name\": \"col\", \"type\": \"string\"} ]\n" + "}";
	
	public static Schema schema = new Schema.Parser().parse(schemaDescription);
	
	public static byte[] getAvroData(String line) throws IOException {
		GenericRecord datum = new GenericData.Record(schema);
		datum.put("col", new Utf8(line));
		return serialize(datum,schema);
	}
	
	public static GenericRecord deserialize(byte[] array,Schema s) throws IOException  {
		GenericDatumReader<GenericRecord> READER = new GenericDatumReader<GenericRecord>(s);
		Decoder de = DecoderFactory.get().binaryDecoder(array, null);
		
		return READER.read(null, de);
	}

	public static byte[] serialize(GenericRecord record,Schema s) throws IOException {
		
		GenericDatumWriter<GenericRecord> wr = new GenericDatumWriter<GenericRecord>(s);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		Encoder e = EncoderFactory.get().binaryEncoder(out, null);
		
		wr.write(record, e);
		e.flush();
		
		return out.toByteArray();
	}
}
