package pl.skidam.mcholepunch.internal.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.junit.jupiter.api.Test;

class MinecraftFrameReaderTest {
	@Test
	void rejectsFramesBeyondTheMaximumLength() {
		ByteArrayOutputStream packet = new ByteArrayOutputStream();
		VarInts.write(packet, 17);
		assertThrows(IOException.class, () -> new MinecraftFrameReader(16).feed(ByteBuffer.wrap(packet.toByteArray())));
	}

	@Test
	void rejectsMalformedLengthVarInts() {
		assertThrows(IOException.class, () -> new MinecraftFrameReader(1024).feed(ByteBuffer.wrap(new byte[]{(byte) 0x80, 0})));
		assertThrows(IOException.class, () -> new MinecraftFrameReader(1024).feed(ByteBuffer.wrap(new byte[]{(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x10})));
		assertThrows(IOException.class, () -> new MinecraftFrameReader(1024).feed(ByteBuffer.wrap(new byte[]{(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0})));
	}

	@Test
	void framesSurviveByteByByteFeeding() throws IOException {
		ByteBuffer frame = MinecraftFrameWriter.frame(new byte[]{7, 7, 7});
		MinecraftFrameReader reader = new MinecraftFrameReader(16);
		List<ByteBuffer> frames = List.of();
		ByteBuffer split = frame.duplicate();
		while (split.hasRemaining()) {
			frames = reader.feed(ByteBuffer.wrap(new byte[]{split.get()}));
		}
		assertEquals(1, frames.size());
		assertEquals(7, frames.get(0).get());
	}
}
