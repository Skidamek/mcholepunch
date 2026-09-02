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
		Reader reader = new Reader();
		for (int index = 0; index < 5; index++) {
			if (!input.hasRemaining()) {
				throw new EOFException("incomplete VarInt");
			}
			if (reader.offer(input.get() & 0xff)) {
				return reader.value();
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

	/** Accumulates VarInt bytes one at a time so the five-byte cap, the 32-bit limit and canonicality stay defined in one place. */
	public static final class Reader {
		private int value;
		private int bytes;

		/** Feeds the next byte and reports whether the VarInt is complete, making {@link #value()} valid. */
		public boolean offer(int next) throws IOException {
			if (bytes == 4 && (next & 0xf0) != 0) {
				throw new IOException("VarInt exceeds 32 bits");
			}
			value |= (next & 0x7f) << (bytes * 7);
			bytes++;
			if ((next & 0x80) == 0) {
				if (encodedLength(value) != bytes) {
					throw new IOException("non-canonical VarInt");
				}
				return true;
			}
			if (bytes == 5) {
				throw new IOException("VarInt exceeds five bytes");
			}
			return false;
		}

		public int value() {
			return value;
		}

		public void reset() {
			value = 0;
			bytes = 0;
		}
	}
}
