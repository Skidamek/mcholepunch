package pl.skidam.mcholepunch.internal.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class MinecraftFrameReader {
	private static final int INITIAL_PENDING_BYTES = 1024;

	private final int maximumFrameLength;
	private final VarInts.Reader reader = new VarInts.Reader();
	private byte[] pending = new byte[INITIAL_PENDING_BYTES];
	private int pendingLength;

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
		append(input);
		List<ByteBuffer> frames = new ArrayList<>();
		int consumed = 0;
		while (frames.size() < maximumFrames) {
			int headerStart = consumed;
			reader.reset();
			boolean complete = false;
			while (consumed < pendingLength) {
				complete = reader.offer(pending[consumed++] & 0xff);
				if (complete) {
					break;
				}
			}
			if (!complete) {
				consumed = headerStart;
				break;
			}
			int length = reader.value();
			if (length < 0 || length > maximumFrameLength) {
				throw new IOException("invalid Minecraft frame length: " + length);
			}
			if (pendingLength - consumed < length) {
				consumed = headerStart;
				break;
			}
			byte[] frame = new byte[length];
			System.arraycopy(pending, consumed, frame, 0, length);
			frames.add(ByteBuffer.wrap(frame));
			consumed += length;
		}
		if (consumed > 0) {
			pendingLength -= consumed;
			System.arraycopy(pending, consumed, pending, 0, pendingLength);
		}
		return frames;
	}

	public ByteBuffer drainPending() {
		byte[] bytes = Arrays.copyOf(pending, pendingLength);
		pendingLength = 0;
		return ByteBuffer.wrap(bytes);
	}

	private void append(ByteBuffer input) {
		int needed = pendingLength + input.remaining();
		if (pending.length < needed) {
			pending = Arrays.copyOf(pending, Math.max(pending.length * 2, needed));
		}
		input.get(pending, pendingLength, input.remaining());
		pendingLength = needed;
	}
}
