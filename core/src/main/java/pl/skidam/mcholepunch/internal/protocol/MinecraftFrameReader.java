package pl.skidam.mcholepunch.internal.protocol;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class MinecraftFrameReader {
	private final ByteArrayOutputStream pending = new ByteArrayOutputStream();
	private final int maximumFrameLength;

	public MinecraftFrameReader(int maximumFrameLength) {
		if (maximumFrameLength <= 0) {
			throw new IllegalArgumentException("maximumFrameLength must be positive");
		}
		this.maximumFrameLength = maximumFrameLength;
	}

	public List<ByteBuffer> feed(ByteBuffer input) throws IOException {
		return feed(input, Integer.MAX_VALUE);
	}

	public List<ByteBuffer> feed(ByteBuffer input, int maximumFrames) throws IOException {
		if (maximumFrames < 1) {
			throw new IllegalArgumentException("maximumFrames must be positive");
		}
		while (input.hasRemaining()) {
			pending.write(input.get());
		}
		List<ByteBuffer> frames = new ArrayList<>();
		while (frames.size() < maximumFrames) {
			byte[] bytes = pending.toByteArray();
			int[] result = readVarInt(bytes, 0);
			if (result == null) {
				break;
			}
			int length = result[0];
			int headerLength = result[1];
			if (length < 0 || length > maximumFrameLength) {
				throw new IOException("invalid Minecraft frame length: " + length);
			}
			if (bytes.length - headerLength < length) {
				break;
			}
			frames.add(ByteBuffer.wrap(bytes, headerLength, length).slice().asReadOnlyBuffer());
			pending.reset();
			pending.write(bytes, headerLength + length, bytes.length - headerLength - length);
		}
		return frames;
	}

	public ByteBuffer drainPending() {
		byte[] bytes = pending.toByteArray();
		pending.reset();
		return ByteBuffer.wrap(bytes).asReadOnlyBuffer();
	}

	public int pendingBytes() {
		return pending.size();
	}

	private static int[] readVarInt(byte[] bytes, int offset) throws IOException {
		int value = 0;
		for (int index = 0; index < 5; index++) {
			if (offset + index >= bytes.length) {
				return null;
			}
			int next = bytes[offset + index] & 0xff;
			if (index == 4 && (next & 0xf0) != 0) {
				throw new IOException("frame length VarInt exceeds 32 bits");
			}
			value |= (next & 0x7f) << index * 7;
			if ((next & 0x80) == 0) {
				if (VarInts.encodedLength(value) != index + 1) {
					throw new IOException("non-canonical frame length VarInt");
				}
				return new int[]{value, index + 1};
			}
		}
		throw new IOException("frame length VarInt exceeds five bytes");
	}
}
