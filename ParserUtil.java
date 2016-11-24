package com.lkl.hbase;

import java.util.ArrayList;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class ParserUtil {
	/**
	 * Column families and qualifiers mapped to the TSV columns
	 */
	private final byte[][] families;
	private final byte[][] qualifiers;

	private final byte separatorByte;

	private int rowKeyColumnIndex;

	public static String ROWKEY_COLUMN_SPEC = "HBASE_ROW_KEY";

	/**
	 * @param columnsSpecification
	 *            the list of columns to parser out, comma separated. The row
	 *            key should be the special token TsvParser.ROWKEY_COLUMN_SPEC
	 */
	public ParserUtil(String columnsSpecification, String separatorStr) {
		// Configure separator
		byte[] separator = Bytes.toBytes(separatorStr);
//		Preconditions.checkArgument(separator.length == 1,
//				"TsvParser only supports single-byte separators");
		separatorByte = separator[0];

		// Configure columns
		ArrayList<String> columnStrings = Lists.newArrayList(Splitter.on(',')
				.trimResults().split(columnsSpecification));

		families = new byte[columnStrings.size()][];
		qualifiers = new byte[columnStrings.size()][];

		for (int i = 0; i < columnStrings.size(); i++) {
			String str = columnStrings.get(i);
			if (ROWKEY_COLUMN_SPEC.equals(str)) {
				rowKeyColumnIndex = i;
				continue;
			}
			String[] parts = str.split(":", 2);
			if (parts.length == 1) {
				families[i] = str.getBytes();
				qualifiers[i] = HConstants.EMPTY_BYTE_ARRAY;
			} else {
				families[i] = parts[0].getBytes();
				qualifiers[i] = parts[1].getBytes();
			}
		}
	}

	public int getRowKeyColumnIndex() {
		return rowKeyColumnIndex;
	}

	public byte[] getFamily(int idx) {
		return families[idx];
	}

	public byte[] getQualifier(int idx) {
		return qualifiers[idx];
	}
	
	public int getQualifierLength(){
		return qualifiers.length;
	}

	public ParsedLine parse(byte[] lineBytes, int length)
			throws BadTsvLineException {
		// Enumerate separator offsets
		ArrayList<Integer> tabOffsets = new ArrayList<Integer>(families.length);
		for (int i = 0; i < length; i++) {
			if (lineBytes[i] == separatorByte) {
				tabOffsets.add(i);
			}
		}
		if (tabOffsets.isEmpty()) {
			throw new BadTsvLineException("No delimiter");
		}

		tabOffsets.add(length);

		if (tabOffsets.size() > families.length) {
			throw new BadTsvLineException("Excessive columns");
		} else if (tabOffsets.size() <= getRowKeyColumnIndex()) {
			throw new BadTsvLineException("No row key");
		}
		return new ParsedLine(tabOffsets, lineBytes);
	}

	class ParsedLine {
		private final ArrayList<Integer> tabOffsets;
		private byte[] lineBytes;

		ParsedLine(ArrayList<Integer> tabOffsets, byte[] lineBytes) {
			this.tabOffsets = tabOffsets;
			this.lineBytes = lineBytes;
		}

		public int getRowKeyOffset() {
			return getColumnOffset(rowKeyColumnIndex);
		}

		public int getRowKeyLength() {
			return getColumnLength(rowKeyColumnIndex);
		}

		public int getColumnOffset(int idx) {
			if (idx > 0)
				return tabOffsets.get(idx - 1) + 1;
			else
				return 0;
		}

		public int getColumnLength(int idx) {
			return tabOffsets.get(idx) - getColumnOffset(idx);
		}

		public int getColumnCount() {
			return tabOffsets.size();
		}

		public byte[] getLineBytes() {
			return lineBytes;
		}
	}

	public static class BadTsvLineException extends Exception {
		public BadTsvLineException(String err) {
			super(err);
		}

		private static final long serialVersionUID = 1L;
	}
}
