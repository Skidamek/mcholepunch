package pl.skidam.mcholepunch.internal.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class TransportFrameReader {
	private final int maximumFrameLength;
	private final VarInts.Reader lengthReader = new VarInts.Reader();
	private Stage stage = Stage.TYPE;
	private TransportFrame.Type type;
	private byte[] payload;
	private int payloadOffset;

	public TransportFrameReader(int maximumFrameLength) {
		if (maximumFrameLength <= 0) {
			throw new IllegalArgumentException("maximumFrameLength must be positive");
		}
		this.maximumFrameLength = maximumFrameLength;
	}

	public int maximumInputBytes(int availableBytes) {
		if (availableBytes <= 0) {
			return 0;
		}
		return stage == Stage.PAYLOAD ? Math.min(availableBytes, payload.length - payloadOffset) : 1;
	}

	public List<TransportFrame> feed(ByteBuffer input) throws IOException {
		List<TransportFrame> frames = new ArrayList<>();
		while (input.hasRemaining()) {
			switch (stage) {
				case TYPE -> readType(input.get() & 0xff);
				case LENGTH -> {
					if (lengthReader.offer(input.get() & 0xff)) {
						TransportFrame frame = frameForLength(lengthReader.value());
						if (frame != null) {
							frames.add(frame);
						}
					}
				}
				case PAYLOAD -> {
					int copied = Math.min(input.remaining(), payload.length - payloadOffset);
					input.get(payload, payloadOffset, copied);
					payloadOffset += copied;
					if (payloadOffset == payload.length) {
						frames.add(new TransportFrame(type, payload));
						reset();
					}
				}
			}
		}
		return frames;
	}

	private void readType(int value) throws IOException {
		type = TransportFrame.Type.of(value);
		if (type == null) {
			throw new IOException("unknown mcholepunch transport frame type: " + value);
		}
		lengthReader.reset();
		stage = Stage.LENGTH;
	}

	private TransportFrame frameForLength(int length) throws IOException {
		if (length < 0 || length > maximumFrameLength) {
			throw new IOException("transport frame is too large: " + length);
		}
		if (type != TransportFrame.Type.DATA && length != 0) {
			throw new IOException("transport control frame must not have a payload");
		}
		if (length == 0) {
			if (type == TransportFrame.Type.DATA) {
				throw new IOException("transport data frame must not be empty");
			}
			payload = new byte[0];
			stage = Stage.TYPE;
			TransportFrame frame = new TransportFrame(type, payload);
			reset();
			return frame;
		}
		payload = new byte[length];
		payloadOffset = 0;
		stage = Stage.PAYLOAD;
		return null;
	}

	private void reset() {
		stage = Stage.TYPE;
		type = null;
		payload = null;
		payloadOffset = 0;
	}

	private enum Stage {
		TYPE,
		LENGTH,
		PAYLOAD
	}
}
