package pl.skidam.mcholepunch.internal.protocol;

import java.nio.ByteBuffer;

public final class TransportFrameCodec {
	private TransportFrameCodec() {}

	public static int dataHeaderLength(int payloadLength) {
		return 1 + VarInts.encodedLength(payloadLength);
	}

	public static void writeDataHeader(ByteBuffer output, int payloadLength) {
		output.put(TransportFrame.Type.DATA.wire());
		VarInts.write(output, payloadLength);
	}

	public static ByteBuffer data(ByteBuffer payload) {
		ByteBuffer input = payload.duplicate();
		ByteBuffer frame = ByteBuffer.allocate(dataHeaderLength(input.remaining()) + input.remaining());
		writeDataHeader(frame, input.remaining());
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
