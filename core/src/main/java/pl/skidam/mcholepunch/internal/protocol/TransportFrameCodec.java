package pl.skidam.mcholepunch.internal.protocol;

import java.nio.ByteBuffer;

public final class TransportFrameCodec {
	private TransportFrameCodec() {}

	public static ByteBuffer data(ByteBuffer payload) {
		ByteBuffer input = payload.duplicate();
		ByteBuffer frame = ByteBuffer.allocate(1 + VarInts.encodedLength(input.remaining()) + input.remaining());
		frame.put((byte) 0);
		VarInts.write(frame, input.remaining());
		frame.put(input);
		return frame.flip();
	}

	public static ByteBuffer ready() {
		return control((byte) 1);
	}

	public static ByteBuffer ack() {
		return control((byte) 2);
	}

	public static ByteBuffer commit() {
		return control((byte) 3);
	}

	private static ByteBuffer control(byte type) {
		return ByteBuffer.wrap(new byte[]{type, 0});
	}
}
