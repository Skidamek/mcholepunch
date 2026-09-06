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
import pl.skidam.mcholepunch.internal.transport.TransportFeed;

public final class NettyTransportDecoder extends ByteToMessageDecoder {
	private final NettyEncryption encryption;
	private final NettyHolepunchConnection connection;
	private final TransportFeed feed;

	public NettyTransportDecoder(NettyEncryption encryption, NettyHolepunchConnection connection, int maximumFrameLength) {
		this.encryption = encryption;
		this.connection = connection;
		this.feed = new TransportFeed(encryption.transport(), new TransportFrameReader(maximumFrameLength));
	}

	@Override
	protected void decode(ChannelHandlerContext context, ByteBuf input, List<Object> output) {
		while (input.isReadable()) {
			if (feed.isRaw()) {
				output.add(input.readRetainedSlice(input.readableBytes()));
				return;
			}
			for (ByteBuffer source : input.nioBuffers(input.readerIndex(), input.readableBytes())) {
				try {
					for (TransportFrame frame : feed.feed(source)) {
						switch (frame.type()) {
							case DATA -> {
								byte[] payload = frame.payload();
								if (payload.length != 0) {
									output.add(Unpooled.wrappedBuffer(payload));
								}
							}
							case COMMIT -> connection.receiveControl(frame.type());
						}
					}
				} catch (IOException exception) {
					throw new DecoderException("invalid mcholepunch transport frame", exception);
				}
				input.skipBytes(source.position());
				if (feed.isRaw()) {
					break;
				}
			}
		}
	}
}
