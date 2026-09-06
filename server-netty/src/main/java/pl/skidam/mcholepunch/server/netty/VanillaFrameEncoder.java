package pl.skidam.mcholepunch.server.netty;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import pl.skidam.mcholepunch.internal.protocol.VarInts;

/**
 * Writes the vanilla-shaped login framing: a canonical VarInt length prefix followed by the
 * packet payload, byte for byte what the vanilla prepender produces.
 */
public final class VanillaFrameEncoder extends MessageToByteEncoder<ByteBuf> {
	public static final String HANDLER_NAME = "mcholepunch-frame-encoder";

	public VanillaFrameEncoder() {
		super(true);
	}

	@Override
	protected void encode(ChannelHandlerContext context, ByteBuf input, ByteBuf output) {
		byte[] prefix = new byte[5];
		ByteBuffer buffer = ByteBuffer.wrap(prefix);
		VarInts.write(buffer, input.readableBytes());
		output.writeBytes(prefix, 0, buffer.position());
		output.writeBytes(input, input.readerIndex(), input.readableBytes());
	}
}
