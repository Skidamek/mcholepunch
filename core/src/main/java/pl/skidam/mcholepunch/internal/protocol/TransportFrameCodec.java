package pl.skidam.mcholepunch.internal.protocol;

import java.nio.ByteBuffer;

public final class TransportFrameCodec {
	private TransportFrameCodec() {}

	public static ByteBuffer data(ByteBuffer payload) {
		ByteBuffer input = payload.duplicate();
		ByteBuffer frame = ByteBuffer.allocate(1 + VarInts.encodedLength(input.remaining()) + input.remaining());
		frame.put(TransportFrame.Type.DATA.wire());
		VarInts.write(frame, input.remaining());
		frame.put(input);
		return frame.flip();
	}

	public static ByteBuffer commit() {
		return control(TransportFrame.Type.COMMIT);
	}

	private static ByteBuffer control(TransportFrame.Type type) {
		return ByteBuffer.wrap(new byte[]{type.wire(), 0});
	}
}
