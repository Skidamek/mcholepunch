package pl.skidam.mcholepunch.server.netty;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.DecoderException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchOptions;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;
import pl.skidam.mcholepunch.internal.transport.TransportHandoff;

/**
 * Owns the transport handoff state of one taken-over channel and doubles as its outbound
 * transport framer: application writes arriving through the pipeline are framed into transport
 * data frames before the handoff and passed through raw after it. Inbound delivery is plain
 * pipeline flow: the transport decoder emits bytes downstream and Netty's own read/writability
 * flow control applies.
 */
public final class NettyHolepunchConnection extends ChannelOutboundHandlerAdapter implements HolepunchConnection {
	/** Pipeline name under which {@link NettyLoginNegotiator} installs this handler. */
	public static final String HANDLER_NAME = "mcholepunch-transport-framer";

	private final long debugId = DebugLog.nextConnectionId();
	private final Channel channel;
	private final NettyEncryption encryption;
	private final long maximumPendingWriteBytes;
	private final int maximumFrameBytes;
	private final boolean automaticRead;
	private final AtomicBoolean closed = new AtomicBoolean();
	private final AtomicLong sentBytes = new AtomicLong();
	private final AtomicReference<CompletableFuture<Void>> commitResult = new AtomicReference<>();
	private final TransportHandoff handoff = new TransportHandoff();
	private final ScheduledFuture<?> watchdog;
	private volatile ChannelHandlerContext framerContext;

	/** Creates a connection with the default send-side frame cap ({@link HolepunchOptions#DEFAULT_MAXIMUM_FRAME_BYTES}). */
	public NettyHolepunchConnection(Channel channel, NettyEncryption encryption, long maximumPendingWriteBytes) {
		this(channel, encryption, maximumPendingWriteBytes, HolepunchOptions.DEFAULT_MAXIMUM_FRAME_BYTES);
	}

	public NettyHolepunchConnection(Channel channel, NettyEncryption encryption, long maximumPendingWriteBytes, int maximumFrameBytes) {
		this.channel = Objects.requireNonNull(channel, "channel");
		this.encryption = Objects.requireNonNull(encryption, "encryption");
		if (maximumPendingWriteBytes <= 0) {
			throw new IllegalArgumentException("maximumPendingWriteBytes must be positive");
		}
		if (maximumFrameBytes < 1 || maximumFrameBytes > TransportFrame.MAX_FRAME_BYTES) {
			throw new IllegalArgumentException("maximumFrameBytes must be between 1 and " + TransportFrame.MAX_FRAME_BYTES);
		}
		this.maximumPendingWriteBytes = maximumPendingWriteBytes;
		this.maximumFrameBytes = maximumFrameBytes;
		this.automaticRead = channel.config().isAutoRead();
		this.watchdog = DebugLog.watchdog("SERVER", debugId, this::debugState);
		DebugLog.log("SERVER", debugId, "connection created local=%s remote=%s active=%s open=%s writable=%s autoRead=%s maxPending=%d maxFrame=%d eventLoop=%s", channel.localAddress(), channel.remoteAddress(),
				channel.isActive(), channel.isOpen(), channel.isWritable(), channel.config().isAutoRead(), maximumPendingWriteBytes, maximumFrameBytes, channel.eventLoop().getClass().getName());
	}

	public long debugId() {
		return debugId;
	}

	public Channel channel() {
		return channel;
	}

	NettyEncryption encryption() {
		return encryption;
	}

	@Override
	public void handlerAdded(ChannelHandlerContext context) {
		framerContext = context;
		channel.config().setWriteBufferWaterMark(new WriteBufferWaterMark(Math.max(1, (int) Math.min(Integer.MAX_VALUE, maximumPendingWriteBytes / 2)), (int) Math.min(Integer.MAX_VALUE, maximumPendingWriteBytes)));
	}

	@Override
	public void write(ChannelHandlerContext context, Object message, ChannelPromise promise) throws Exception {
		if (!(message instanceof ByteBuf input)) {
			// A packet-shaped write from leftover vanilla state must not fail the connection, but silent
			// success is how a stale write goes unnoticed, so leave a receipt when it happens.
			DebugLog.log("SERVER", debugId, "dropping non-buffer write %s", message.getClass().getName());
			ReferenceCountUtil.release(message);
			promise.setSuccess();
			return;
		}
		if (!input.isReadable()) {
			// Vanilla's tick flush produces empty writes; the transport forbids empty data frames, so they complete as no-ops.
			input.release();
			promise.setSuccess();
			return;
		}
		if (closed.get()) {
			ReferenceCountUtil.release(input);
			promise.setFailure(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
			return;
		}
		if (handoff.isCommitting()) {
			ReferenceCountUtil.release(input);
			promise.setFailure(new HolepunchFailure(HolepunchFailure.Kind.IO, "transport commit in flight", null));
			return;
		}
		if (handoff.isRaw()) {
			DebugLog.log("SERVER", debugId, "raw pass-through write bytes=%d writable=%s", input.readableBytes(), channel.isWritable());
			context.write(input, promise);
			return;
		}
		int size = input.readableBytes();
		if (size <= maximumFrameBytes) {
			ByteBuf frame = Unpooled.wrappedBuffer(TransportFrameCodec.data(input.nioBuffer()));
			input.release();
			DebugLog.log("SERVER", debugId, "framed write bytes=%d writable=%s", frame.readableBytes(), channel.isWritable());
			context.write(frame, promise);
			return;
		}
		// The peer accepts up to the protocol ceiling, but the caller's send cap bounds the per-frame
		// buffer; one write above it is split. The caller's promise fails if any chunk fails and
		// completes once all are on the wire.
		PromiseCombiner combiner = new PromiseCombiner(context.executor());
		try {
			int offset = 0;
			while (offset < size) {
				int chunkBytes = Math.min(size - offset, maximumFrameBytes);
				ByteBuf frame = Unpooled.wrappedBuffer(TransportFrameCodec.data(input.nioBuffer(input.readerIndex() + offset, chunkBytes)));
				offset += chunkBytes;
				combiner.add(context.write(frame, context.newPromise()));
			}
			combiner.finish(promise);
		} finally {
			input.release();
		}
		DebugLog.log("SERVER", debugId, "framed write split chunks=%d bytes=%d writable=%s", (size + maximumFrameBytes - 1) / maximumFrameBytes, size, channel.isWritable());
	}

	@Override
	public void flush(ChannelHandlerContext context) {
		context.flush();
	}

	@Override
	public CompletionStage<Void> write(ByteBuffer data) {
		if (data == null) {
			return CompletableFuture.failedFuture(new NullPointerException("data"));
		}
		ByteBuffer copy = data.duplicate();
		int size = copy.remaining();
		if (size == 0) {
			return CompletableFuture.completedFuture(null);
		}
		if (closed.get()) {
			return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
		}
		if (handoff.isCommitting()) {
			return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.IO, "transport commit in flight", null));
		}
		CompletableFuture<Void> result = new CompletableFuture<>();
		// The framer owns the era checks and the framing; writing through the channel keeps one
		// copy of those rules for pipeline writes and this API alike.
		channel.writeAndFlush(Unpooled.wrappedBuffer(copy)).addListener((ChannelFutureListener) completed -> {
			if (!completed.isSuccess()) {
				Throwable cause = completed.cause();
				if (cause instanceof HolepunchFailure failure) {
					// The framer rejected this call (commit window or already closed). That is policy
					// for this one write, not a transport error: fail the write and leave the connection.
					DebugLog.log("SERVER", debugId, "write rejected: %s", failure.getMessage());
					result.completeExceptionally(failure);
					return;
				}
				DebugLog.failure("SERVER", debugId, "Netty write failed", cause);
				result.completeExceptionally(cause);
				closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "write failed", cause));
				return;
			}
			if (DebugLog.enabled()) {
				DebugLog.log("SERVER", debugId, "write complete plaintext=%d totalSent=%d writable=%s", size, sentBytes.addAndGet(size), channel.isWritable());
			}
			result.complete(null);
		});
		return result;
	}

	@Override
	public CompletionStage<Void> commitTransportUpgrade() {
		if (closed.get()) {
			return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
		}
		if (handoff.isRaw()) {
			return CompletableFuture.completedFuture(null);
		}
		CompletableFuture<Void> existing = commitResult.get();
		if (existing != null) {
			return existing;
		}
		CompletableFuture<Void> result = new CompletableFuture<>();
		if (!commitResult.compareAndSet(null, result)) {
			return commitResult.get();
		}
		try {
			executeOnEventLoop(() -> {
				if (closed.get()) {
					result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
					return;
				}
				if (framerContext == null) {
					result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.IO, "transport framer is not installed", null));
					return;
				}
				if (handoff.requestCommit()) {
					writeCommitControl();
				}
			});
		} catch (RejectedExecutionException rejected) {
			// A shutting-down event loop rejects the commit task; the stored future must not outlive
			// the connection uncompleted, so finish it here and close.
			HolepunchFailure failure = new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null);
			result.completeExceptionally(failure);
			closeWith(failure);
		}
		return result;
	}

	void receiveControl() {
		executeOnEventLoop(() -> {
			if (closed.get() || handoff.isRaw()) {
				return;
			}
			if (handoff.onPeerCommitReceived()) {
				activateRaw();
			}
		});
	}

	private void writeCommitControl() {
		ChannelHandlerContext context = framerContext;
		if (context == null) {
			closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "transport framer is not installed", null));
			return;
		}
		DebugLog.log("SERVER", debugId, "event-loop writeAndFlush kind=COMMIT writable=%s", channel.isWritable());
		context.writeAndFlush(Unpooled.wrappedBuffer(TransportFrameCodec.commit())).addListener((ChannelFutureListener) completed -> {
			if (!completed.isSuccess()) {
				DebugLog.failure("SERVER", debugId, "Netty control write failed", completed.cause());
				closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "control write failed", completed.cause()));
				return;
			}
			if (handoff.onCommitSent()) {
				activateRaw();
			}
		});
	}

	/** Flips the cipher and any committer to the raw era; called exactly once, when both COMMITs have crossed. */
	private void activateRaw() {
		encryption.activateRaw();
		DebugLog.log("SERVER", debugId, "transport switched to raw pass-through");
		CompletableFuture<Void> result = commitResult.get();
		if (result != null) result.complete(null);
	}

	@Override
	public byte[] transportSecret() {
		return encryption.transportSecret();
	}

	@Override
	public boolean isRaw() {
		return handoff.isRaw();
	}

	@Override
	public void pauseReads() {
		executeOnEventLoop(() -> channel.config().setAutoRead(false));
	}

	@Override
	public void resumeReads() {
		executeOnEventLoop(() -> {
			if (!closed.get() && automaticRead) {
				channel.config().setAutoRead(true);
			}
		});
	}

	@Override
	public void close() {
		closeWith(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
	}

	public void closeWith(HolepunchFailure failure) {
		if (!closed.compareAndSet(false, true)) {
			DebugLog.log("SERVER", debugId, "ignoring duplicate close: %s", failure);
			return;
		}
		DebugLog.failure("SERVER", debugId, "closing connection kind=" + failure.kind(), failure);
		DebugLog.cancel(watchdog);
		CompletableFuture<Void> commit = commitResult.get();
		if (commit != null) commit.completeExceptionally(failure);
		channel.close();
	}

	private void executeOnEventLoop(Runnable action) {
		if (channel.eventLoop().inEventLoop()) {
			action.run();
		} else {
			channel.eventLoop().execute(action);
		}
	}

	private String debugState() {
		return String.format(Locale.ROOT, "active=%s open=%s registered=%s writable=%s autoRead=%s phase=%s raw=%s sent=%d beforeWritable=%d beforeUnwritable=%d pipeline=%s", channel.isActive(),
				channel.isOpen(), channel.isRegistered(), channel.isWritable(), channel.config().isAutoRead(), handoff.phase(), encryption.isRaw(), sentBytes.get(), channel.bytesBeforeWritable(),
				channel.bytesBeforeUnwritable(), channel.pipeline().names());
	}

	/**
	 * Closes the channel on an inbound pipeline failure the application did not handle, and is
	 * installed tail-ward of the application handlers so it also catches exceptions they let
	 * through. A malformed frame or a dropped socket is the peer's doing and the app only sees the
	 * closed channel, so it records under tracing; anything else is an internal error that reaches
	 * no caller and prints as a hard error rather than disappearing into a silent close.
	 */
	static final class InboundWatchdog extends ChannelInboundHandlerAdapter {
		private final NettyHolepunchConnection connection;

		InboundWatchdog(NettyHolepunchConnection connection) {
			this.connection = connection;
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
			if (isPeerFrameFailure(cause)) {
				DebugLog.failure("SERVER", connection.debugId, "pipeline exception", cause);
			} else {
				DebugLog.hardError("SERVER", connection.debugId, "pipeline exception", cause);
			}
			context.close();
		}

		/**
		 * Whether the pipeline failure is the peer's doing: a malformed frame surfaces as the frame
		 * reader's IOException at the root of a DecoderException. Any other cause inside a
		 * DecoderException, and any raw exception reaching the tail, is a library or application
		 * fault that reaches no caller and must print.
		 */
		private static boolean isPeerFrameFailure(Throwable cause) {
			if (!(cause instanceof DecoderException)) {
				return false;
			}
			Throwable root = cause;
			while (root.getCause() != null) {
				root = root.getCause();
			}
			return root instanceof IOException;
		}
	}
}
