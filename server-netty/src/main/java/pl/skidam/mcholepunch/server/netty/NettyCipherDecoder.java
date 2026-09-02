package pl.skidam.mcholepunch.server.netty;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public final class NettyCipherDecoder extends ByteToMessageDecoder {
	private final NettyEncryption encryption;

	public NettyCipherDecoder(NettyEncryption encryption) {
		this.encryption = encryption;
	}

	@Override
	protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {
		if (!input.isReadable()) {
			return;
		}
		int readableBytes = input.readableBytes();
		ByteBuf decrypted = context.alloc().buffer(readableBytes);
		try {
			encryption.decrypt(input, decrypted);
			input.skipBytes(readableBytes);
			if (decrypted.isReadable()) {
				output.add(decrypted);
			} else {
				decrypted.release();
			}
		} catch (Throwable throwable) {
			decrypted.release();
			throw throwable;
		}
	}
}
