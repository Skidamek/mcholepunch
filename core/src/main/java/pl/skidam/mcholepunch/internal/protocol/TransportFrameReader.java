package pl.skidam.mcholepunch.internal.protocol;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class TransportFrameReader {
	private final int maximumFrameLength;
	private Stage stage = Stage.TYPE;
	private TransportFrame.Type type;
	private int length;
	private int lengthBytes;
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
					int next = input.get() & 0xff;
					TransportFrame frame = readLengthByte(next);
					if (frame != null) {
						frames.add(frame);
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
		type = switch (value) {
			case 0 -> TransportFrame.Type.DATA;
			case 1 -> TransportFrame.Type.READY;
			case 2 -> TransportFrame.Type.ACK;
			case 3 -> TransportFrame.Type.COMMIT;
			default -> throw new IOException("unknown mcholepunch transport frame type: " + value);
		};
		length = 0;
		lengthBytes = 0;
		stage = Stage.LENGTH;
	}

	private TransportFrame readLengthByte(int next) throws IOException {
		if (lengthBytes == 4 && (next & 0xf0) != 0) {
			throw new IOException("transport frame length VarInt exceeds 32 bits");
		}
		length |= (next & 0x7f) << lengthBytes * 7;
		lengthBytes++;
		if ((next & 0x80) != 0) {
			if (lengthBytes == 5) {
				throw new IOException("transport frame length VarInt exceeds five bytes");
			}
			return null;
		}
		if (VarInts.encodedLength(length) != lengthBytes) {
			throw new IOException("non-canonical transport frame length VarInt");
		}
		if (length < 0 || length > maximumFrameLength) {
			throw new IOException("transport frame is too large: " + length);
		}
		if ((type == TransportFrame.Type.READY || type == TransportFrame.Type.ACK || type == TransportFrame.Type.COMMIT) && length != 0) {
			throw new IOException("transport control frame must not have a payload");
		}
		if (length == 0) {
			payload = new byte[0];
			stage = Stage.TYPE;
			TransportFrame frame = new TransportFrame(type, payload);
			reset();
			return frame;
		} else {
			payload = new byte[length];
			payloadOffset = 0;
			stage = Stage.PAYLOAD;
		}
		return null;
	}

	private void reset() {
		stage = Stage.TYPE;
		type = null;
		length = 0;
		lengthBytes = 0;
		payload = null;
		payloadOffset = 0;
	}

	private enum Stage {
		TYPE,
		LENGTH,
		PAYLOAD
	}
}
