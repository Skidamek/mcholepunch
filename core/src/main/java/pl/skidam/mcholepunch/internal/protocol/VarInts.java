package pl.skidam.mcholepunch.internal.protocol;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class VarInts {
	private VarInts() {}

	public static void write(ByteArrayOutputStream output, int value) {
		int current = value;
		do {
			int next = current >>> 7;
			output.write((current & 0x7f) | (next == 0 ? 0 : 0x80));
			current = next;
		} while (current != 0);
	}

	public static void write(ByteBuffer output, int value) {
		int current = value;
		do {
			int next = current >>> 7;
			output.put((byte) ((current & 0x7f) | (next == 0 ? 0 : 0x80)));
			current = next;
		} while (current != 0);
	}

	public static int read(ByteBuffer input) throws IOException {
		int value = 0;
		for (int index = 0; index < 5; index++) {
			if (!input.hasRemaining()) {
				throw new EOFException("incomplete VarInt");
			}
			int next = input.get() & 0xff;
			if (index == 4 && (next & 0xf0) != 0) {
				throw new IOException("VarInt exceeds 32 bits");
			}
			value |= (next & 0x7f) << (index * 7);
			if ((next & 0x80) == 0) {
				if (encodedLength(value) != index + 1) {
					throw new IOException("non-canonical VarInt");
				}
				return value;
			}
		}
		throw new IOException("VarInt exceeds five bytes");
	}

	public static int encodedLength(int value) {
		int length = 1;
		int current = value >>> 7;
		while (current != 0) {
			length++;
			current >>>= 7;
		}
		return length;
	}
}
