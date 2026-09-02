package pl.skidam.mcholepunch.internal.protocol;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public final class MinecraftFrameWriter {
	private MinecraftFrameWriter() {}

	public static ByteBuffer frame(byte[] packet) {
		ByteArrayOutputStream output = new ByteArrayOutputStream(packet.length + 5);
		VarInts.write(output, packet.length);
		output.writeBytes(packet);
		return ByteBuffer.wrap(output.toByteArray());
	}
}
