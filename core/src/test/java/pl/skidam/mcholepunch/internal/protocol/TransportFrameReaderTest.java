package pl.skidam.mcholepunch.internal.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class TransportFrameReaderTest {
	@Test
	void readsSplitDataAndControlFrames() throws Exception {
		ByteBuffer data = TransportFrameCodec.data(ByteBuffer.wrap(new byte[]{1, 2, 3}));
		ByteBuffer ready = TransportFrameCodec.ready();
		ByteBuffer ack = TransportFrameCodec.ack();
		ByteBuffer commit = TransportFrameCodec.commit();
		byte[] encoded = concat(data, ready, ack, commit);
		TransportFrameReader reader = new TransportFrameReader(1024);
		List<TransportFrame> frames = new ArrayList<>();

		for (byte value : encoded) {
			frames.addAll(reader.feed(ByteBuffer.wrap(new byte[]{value})));
		}

		assertEquals(List.of(TransportFrame.Type.DATA, TransportFrame.Type.READY, TransportFrame.Type.ACK, TransportFrame.Type.COMMIT), frames.stream().map(TransportFrame::type).toList());
		assertArrayEquals(new byte[]{1, 2, 3}, frames.get(0).payload());
		assertEquals(0, reader.maximumInputBytes(0));
	}

	@Test
	void rejectsInvalidControlLength() {
		assertThrows(IOException.class, () -> new TransportFrameReader(1024).feed(ByteBuffer.wrap(new byte[]{1, 1, 4})));
		assertThrows(IOException.class, () -> new TransportFrameReader(1024).feed(ByteBuffer.wrap(new byte[]{3, 1, 4})));
		assertThrows(IOException.class, () -> new TransportFrameReader(1024).feed(ByteBuffer.wrap(new byte[]{0, (byte) 0x80, 0})));
	}

	private static byte[] concat(ByteBuffer... buffers) {
		int size = 0;
		for (ByteBuffer buffer : buffers) size += buffer.remaining();
		ByteBuffer result = ByteBuffer.allocate(size);
		for (ByteBuffer buffer : buffers) result.put(buffer.duplicate());
		return result.array();
	}
}
