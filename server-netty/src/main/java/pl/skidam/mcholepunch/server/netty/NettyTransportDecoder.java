package pl.skidam.mcholepunch.server.netty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;

import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameReader;

public final class NettyTransportDecoder extends ByteToMessageDecoder {
	private final NettyEncryption encryption;
	private final NettyHolepunchConnection connection;
	private final TransportFrameReader frameReader;

	public NettyTransportDecoder(NettyEncryption encryption, NettyHolepunchConnection connection, int maximumFrameLength) {
		this.encryption = encryption;
		this.connection = connection;
		this.frameReader = new TransportFrameReader(maximumFrameLength);
	}

	@Override
	protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {
		while (input.isReadable()) {
			if (encryption.isRaw()) {
				output.add(new NettyHolepunchConnection.RawInbound(input.readRetainedSlice(input.readableBytes())));
				return;
			}
			int encryptedBytes = frameReader.maximumInputBytes(input.readableBytes());
			ByteBuf encrypted = input.readSlice(encryptedBytes);
			ByteBuf plaintext = context.alloc().buffer(encryptedBytes);
			try {
				encryption.decrypt(encrypted, plaintext);
				ByteBuffer bytes = plaintext.nioBuffer(plaintext.readerIndex(), plaintext.readableBytes());
				for (TransportFrame frame : frameReader.feed(bytes)) {
					switch (frame.type()) {
						case DATA -> {
							byte[] payload = frame.payload();
							if (payload.length != 0) {
								output.add(Unpooled.wrappedBuffer(payload));
							}
						}
						case READY, ACK, COMMIT -> connection.receiveControl(frame.type());
					}
				}
			} catch (IOException exception) {
				throw new DecoderException("invalid mcholepunch transport frame", exception);
			} finally {
				plaintext.release();
			}
		}
	}
}
