package pl.skidam.mcholepunch.server.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.MessageToByteEncoder;

/** Encrypts whole framed packets once the negotiation keys the cipher; the raw stream passes through untouched. */
public final class NettyCipherEncoder extends MessageToByteEncoder<ByteBuf> {
	private final NettyEncryption encryption;

	public NettyCipherEncoder(NettyEncryption encryption) {
		super(true);
		this.encryption = encryption;
	}

	@Override
	public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) throws Exception {
		if (encryption.isRaw()) {
			context.write(message, promise);
			return;
		}
		super.write(context, message, promise);
	}

	@Override
	protected void encode(ChannelHandlerContext context, ByteBuf input, ByteBuf output) {
		encryption.encrypt(input, output);
	}
}
