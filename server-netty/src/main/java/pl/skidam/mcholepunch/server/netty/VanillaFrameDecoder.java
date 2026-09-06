package pl.skidam.mcholepunch.server.netty;

import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import pl.skidam.mcholepunch.internal.protocol.MinecraftFrameReader;

/**
 * Splits the vanilla-shaped login framing (a canonical VarInt length followed by the packet
 * payload) during the negotiation phase, byte for byte what the vanilla frame decoder produces.
 * Replaced by the transport decoder once the takeover query is answered.
 */
public final class VanillaFrameDecoder extends ByteToMessageDecoder {
	public static final String HANDLER_NAME = "mcholepunch-frame-decoder";

	private final MinecraftFrameReader reader;

	public VanillaFrameDecoder(int maximumFrameLength) {
		this.reader = new MinecraftFrameReader(maximumFrameLength);
	}

	@Override
	protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) throws Exception {
		byte[] bytes = new byte[input.readableBytes()];
		input.readBytes(bytes);
		for (ByteBuffer frame : reader.feed(ByteBuffer.wrap(bytes))) {
			output.add(Unpooled.wrappedBuffer(frame));
		}
	}
}
