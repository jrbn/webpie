package utils;

import java.nio.ByteBuffer;

public class NumberUtils {

	public static long decodeLong(byte[] value, int start) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		return buffer.getLong(start);
		/*
		 * return ((long)value[start] << 56) + ((long)(value[start + 1] & 0xFF)
		 * << 48) + ((long)(value[start + 2] & 0xFF) << 40) +
		 * ((long)(value[start + 3] & 0xFF) << 32) + ((value[start + 4] & 0xFF)
		 * << 24) + ((value[start + 5] & 0xFF) << 16) + ((value[start + 6] &
		 * 0xFF) << 8) + (value[start + 7] & 0xFF);
		 */
	}

	public static void encodeLong(byte[] value, int start, long number) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		buffer.putLong(start, number);

		/*
		 * value[start] = (byte)(number >>> 56); value[start + 1] =
		 * (byte)(number >>> 48); value[start + 2] = (byte)(number >>> 40);
		 * value[start + 3] = (byte)(number >>> 32); value[start + 4] =
		 * (byte)(number >>> 24); value[start + 5] = (byte)(number >>> 16);
		 * value[start + 6] = (byte)(number >>> 8); value[start + 7] =
		 * (byte)(number);
		 */
	}

	public static int decodeInt(byte[] value, int start) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		return buffer.getInt(start);
		// return (value[start] << 24) + ((value[start + 1] & 0xFF) << 16)
		// + ((value[start + 2] & 0xFF) << 8) + (value[start + 3] & 0xFF);
	}

	public static void encodeInt(byte[] value, int start, int number) {
		ByteBuffer buffer = ByteBuffer.wrap(value);
		buffer.putInt(start, number);
		/*
		 * value[start] = (byte)(number >>> 24); value[start + 1] =
		 * (byte)(number >>> 16); value[start + 2] = (byte)(number >>> 8);
		 * value[start + 3] = (byte)(number);
		 */
	}
}
