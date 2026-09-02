package pl.skidam.mcholepunch.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public final class NettyCipherEncoder extends MessageToByteEncoder<ByteBuf> {
	private final NettyEncryption encryption;

	public NettyCipherEncoder(NettyEncryption encryption) {
		super(true);
		this.encryption = encryption;
	}

	@Override
	protected void encode(ChannelHandlerContext context, ByteBuf input, ByteBuf output) {
		encryption.encrypt(input, output);
	}
}
