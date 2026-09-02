package pl.skidam.mcholepunch.server.netty;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchHandler;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.concurrent.SerialExecutor;
import pl.skidam.mcholepunch.internal.protocol.TransportFrame;
import pl.skidam.mcholepunch.internal.protocol.TransportFrameCodec;
import pl.skidam.mcholepunch.internal.transport.TransportHandoff;

public final class NettyHolepunchConnection implements HolepunchConnection {
	private final long debugId = DebugLog.nextConnectionId();
	private final Channel channel;
	private final Executor callbackExecutor;
	private final HolepunchHandler handler;
	private final long maximumPendingBytes;
	private final NettyEncryption encryption;
	private final boolean automaticRead;
	private final AtomicLong pendingBytes = new AtomicLong();
	private final AtomicBoolean closed = new AtomicBoolean();
	private final AtomicLong receivedBytes = new AtomicLong();
	private final AtomicLong sentBytes = new AtomicLong();
	private final Deque<OutboundRequest> writes = new ArrayDeque<>();
	private final Deque<PendingRead> pendingReads = new ArrayDeque<>();
	private final AtomicReference<CompletableFuture<Void>> prepareResult = new AtomicReference<>();
	private final AtomicReference<CompletableFuture<Void>> commitResult = new AtomicReference<>();
	private final ScheduledFuture<?> watchdog;
	private boolean writing;
	private boolean applicationReadsPaused;
	private boolean readDeliveryInFlight;
	private final TransportHandoff handoff = new TransportHandoff();

	public NettyHolepunchConnection(Channel channel, Executor callbackExecutor, HolepunchHandler handler, long maximumPendingBytes, NettyEncryption encryption) {
		this.channel = channel;
		// Lifecycle callbacks must stay ordered even when the supplied executor is concurrent.
		// In particular, application bytes may arrive immediately after takeover; delivering
		// onRead before onOpen can make handlers drop the beginning of the stream.
		this.callbackExecutor = new SerialExecutor(callbackExecutor);
		this.handler = handler;
		this.maximumPendingBytes = maximumPendingBytes;
		this.encryption = encryption;
		this.automaticRead = channel.config().isAutoRead();
		this.watchdog = DebugLog.watchdog("SERVER", debugId, this::debugState);
		DebugLog.log("SERVER", debugId, "connection created local=%s remote=%s active=%s open=%s writable=%s autoRead=%s " + "maxPending=%d eventLoop=%s pipeline=%s", channel.localAddress(), channel.remoteAddress(),
				channel.isActive(), channel.isOpen(), channel.isWritable(), channel.config().isAutoRead(), maximumPendingBytes, channel.eventLoop().getClass().getName(), channel.pipeline().names());
	}

	public long debugId() {
		return debugId;
	}

	public Channel channel() {
		return channel;
	}

	public HolepunchHandler handler() {
		return handler;
	}

	@Override
	public boolean isRaw() {
		return handoff.isRaw();
	}

	NettyEncryption encryption() {
		return encryption;
	}

	int maximumFrameLength() {
		return Math.toIntExact(Math.min(Integer.MAX_VALUE, maximumPendingBytes));
	}

	public void open() {
		callbackExecutor.execute(() -> {
			try {
				DebugLog.log("SERVER", debugId, "dispatch onOpen");
				handler.onOpen(this);
				DebugLog.log("SERVER", debugId, "onOpen completed");
			} catch (Throwable throwable) {
				closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "onOpen failed", throwable));
			}
		});
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
		if (!reserve(size)) {
			DebugLog.log("SERVER", debugId, "write rejected by pending limit bytes=%d pending=%d limit=%d", size, pendingBytes.get(), maximumPendingBytes);
			return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.IO, "pending write limit exceeded", null));
		}
		CompletableFuture<Void> result = new CompletableFuture<>();
		DebugLog.log("SERVER", debugId, "write reserved bytes=%d pending=%d; scheduling on event loop", size, pendingBytes.get());
		try {
			channel.eventLoop().execute(() -> {
				if (closed.get()) {
					pendingBytes.addAndGet(-size);
					result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
					return;
				}
				if (handoff.blocksData()) {
					pendingBytes.addAndGet(-size);
					result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.IO, "transport upgrade in progress", null));
					return;
				}
				boolean rawEra = handoff.isRaw();
				writes.addLast(new OutboundRequest(copy, size, result, rawEra, Kind.DATA));
				DebugLog.log("SERVER", debugId, "write queued queueDepth=%d pending=%d rawEra=%s", writes.size(), pendingBytes.get(), rawEra);
				drainWrites();
			});
		} catch (Throwable throwable) {
			pendingBytes.addAndGet(-size);
			result.completeExceptionally(throwable);
		}
		return result;
	}

	@Override
	public CompletionStage<Void> prepareTransportUpgrade() {
		if (encryption == null) {
			return CompletableFuture.failedFuture(new IllegalStateException("transport encryption is not installed"));
		}
		if (closed.get()) {
			return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
		}
		if (handoff.isRaw()) {
			return CompletableFuture.completedFuture(null);
		}
		CompletableFuture<Void> result = new CompletableFuture<>();
		CompletableFuture<Void> existing = prepareResult.get();
		if (existing != null) {
			return existing;
		}
		if (!prepareResult.compareAndSet(null, result)) {
			return prepareResult.get();
		}
		try {
			channel.eventLoop().execute(() -> {
				if (closed.get()) {
					result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
					return;
				}
				TransportHandoff.Transition transition = handoff.requestPrepare();
				applyTransition(transition);
				completeTransition(transition);
				drainWrites();
			});
		} catch (Throwable throwable) {
			result.completeExceptionally(throwable);
		}
		return result;
	}

	@Override
	public CompletionStage<Void> commitTransportUpgrade() {
		if (encryption == null) {
			return CompletableFuture.failedFuture(new IllegalStateException("transport encryption is not installed"));
		}
		if (closed.get()) {
			return CompletableFuture.failedFuture(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
		}
		if (handoff.isRaw()) {
			return CompletableFuture.completedFuture(null);
		}
		CompletableFuture<Void> result = new CompletableFuture<>();
		CompletableFuture<Void> existing = commitResult.get();
		if (existing != null) {
			return existing;
		}
		if (!commitResult.compareAndSet(null, result)) {
			return commitResult.get();
		}
		try {
			channel.eventLoop().execute(() -> {
				if (closed.get()) {
					result.completeExceptionally(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "connection closed", null));
					return;
				}
				try {
					TransportHandoff.Transition transition = handoff.requestCommit();
					applyTransition(transition);
					completeTransition(transition);
					drainWrites();
				} catch (IllegalStateException exception) {
					result.completeExceptionally(exception);
				}
			});
		} catch (Throwable throwable) {
			result.completeExceptionally(throwable);
		}
		return result;
	}

	@Override
	public void pauseReads() {
		executeOnEventLoop(() -> {
			applicationReadsPaused = true;
			channel.config().setAutoRead(false);
		});
	}

	@Override
	public void resumeReads() {
		executeOnEventLoop(() -> {
			applicationReadsPaused = false;
			resumeAutomaticReads();
		});
	}

	void receiveControl(TransportFrame.Type type) {
		executeOnEventLoop(() -> {
			if (closed.get() || encryption == null || handoff.isRaw()) {
				return;
			}
			try {
				TransportHandoff.Transition transition = handoff.onPeerControlReceived(type);
				applyTransition(transition);
				completeTransition(transition);
				drainWrites();
			} catch (IllegalStateException exception) {
				closeWith(new HolepunchFailure(HolepunchFailure.Kind.UNEXPECTED_PACKET, "invalid transport handoff control " + type, exception));
			}
		});
	}

	private void drainWrites() {
		if (!channel.eventLoop().inEventLoop() || writing || closed.get()) {
			return;
		}
		OutboundRequest request = writes.pollFirst();
		if (request == null) {
			return;
		}
		writing = true;
		ByteBuffer frame = switch (request.kind()) {
			case DATA -> request.rawEra() ? request.data().duplicate() : TransportFrameCodec.data(request.data());
			case READY -> TransportFrameCodec.ready();
			case ACK -> TransportFrameCodec.ack();
			case COMMIT -> TransportFrameCodec.commit();
		};
		DebugLog.log("SERVER", debugId, "event-loop writeAndFlush kind=%s plaintext=%d active=%s open=%s writable=%s beforeUnwritable=%d rawEra=%s", request.kind(), frame.remaining(), channel.isActive(),
				channel.isOpen(),
				channel.isWritable(), channel.bytesBeforeUnwritable(), request.rawEra());
		ChannelFuture future = channel.writeAndFlush(Unpooled.wrappedBuffer(frame));
		future.addListener((ChannelFutureListener) completed -> {
			writing = false;
			if (request.kind() == Kind.DATA) {
				pendingBytes.addAndGet(-request.size());
			}
			if (!completed.isSuccess()) {
				DebugLog.failure("SERVER", debugId, "Netty write failed", completed.cause());
				request.result().completeExceptionally(completed.cause());
				closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "write failed", completed.cause()));
				return;
			}
			if (request.kind() == Kind.DATA) {
				long total = sentBytes.addAndGet(request.size());
				DebugLog.log("SERVER", debugId, "write complete bytes=%d totalSent=%d pending=%d writable=%s", request.size(), total, pendingBytes.get(), channel.isWritable());
			} else {
				try {
					TransportHandoff.Transition transition = handoff.onLocalControlSent(toFrameType(request.kind()));
					applyTransition(transition);
					completeTransition(transition);
				} catch (IllegalStateException exception) {
					request.result().completeExceptionally(exception);
					closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "invalid local transport handoff control", exception));
					return;
				}
			}
			request.result().complete(null);
			drainWrites();
		});
	}

	private void applyTransition(TransportHandoff.Transition transition) {
		for (TransportFrame.Type type : transition.outbound()) {
			writes.addLast(new OutboundRequest(null, 0, new CompletableFuture<>(), false, fromFrameType(type)));
		}
		if (transition.activatesRaw()) {
			encryption.activateRaw();
			DebugLog.log("SERVER", debugId, "transport switched to raw pass-through");
		}
	}

	private void completeTransition(TransportHandoff.Transition transition) {
		if (transition.prepared()) {
			CompletableFuture<Void> result = prepareResult.get();
			if (result != null) result.complete(null);
		}
		if (transition.committed()) {
			CompletableFuture<Void> result = commitResult.get();
			if (result != null) result.complete(null);
		}
	}

	private static TransportFrame.Type toFrameType(Kind kind) {
		return switch (kind) {
			case READY -> TransportFrame.Type.READY;
			case ACK -> TransportFrame.Type.ACK;
			case COMMIT -> TransportFrame.Type.COMMIT;
			case DATA -> throw new IllegalArgumentException("DATA is not a handoff control frame");
		};
	}

	private static Kind fromFrameType(TransportFrame.Type type) {
		return switch (type) {
			case READY -> Kind.READY;
			case ACK -> Kind.ACK;
			case COMMIT -> Kind.COMMIT;
			case DATA -> throw new IllegalArgumentException("DATA is not a handoff control frame");
		};
	}

	private boolean reserve(int bytes) {
		while (true) {
			long current = pendingBytes.get();
			if (current > maximumPendingBytes - bytes) {
				return false;
			}
			if (pendingBytes.compareAndSet(current, current + bytes)) {
				return true;
			}
		}
	}

	private void executeOnEventLoop(Runnable action) {
		if (channel.eventLoop().inEventLoop()) {
			action.run();
		} else {
			channel.eventLoop().execute(action);
		}
	}

	private void dispatchRead(ByteBuffer buffer) {
		dispatchRead(buffer, false);
	}

	void dispatchRawRead(ByteBuffer buffer) {
		dispatchRead(buffer, true);
	}

	private void dispatchRead(ByteBuffer buffer, boolean rawEra) {
		if (closed.get()) return;
		if (readDeliveryInFlight) {
			pendingReads.addLast(new PendingRead(buffer, rawEra));
			return;
		}
		deliverRead(new PendingRead(buffer, rawEra));
	}

	private void deliverRead(PendingRead pending) {
		readDeliveryInFlight = true;
		channel.config().setAutoRead(false);
		try {
			callbackExecutor.execute(() -> {
				try {
					if (pending.rawEra()) {
						DebugLog.log("SERVER", debugId, "dispatch onRawRead %s", DebugLog.preview(pending.data()));
						handler.onRawRead(pending.data());
					} else {
						DebugLog.log("SERVER", debugId, "dispatch onRead %s", DebugLog.preview(pending.data()));
						handler.onRead(pending.data());
					}
				} catch (Throwable throwable) {
					closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, pending.rawEra() ? "onRawRead failed" : "onRead failed", throwable));
				} finally {
					executeOnEventLoop(this::finishReadDelivery);
				}
			});
		} catch (Throwable throwable) {
			readDeliveryInFlight = false;
			closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "read scheduling failed", throwable));
		}
	}

	private void finishReadDelivery() {
		if (closed.get()) {
			pendingReads.clear();
			readDeliveryInFlight = false;
			return;
		}
		PendingRead next = pendingReads.pollFirst();
		if (next != null) {
			deliverRead(next);
			return;
		}
		readDeliveryInFlight = false;
		resumeAutomaticReads();
	}

	private void resumeAutomaticReads() {
		if (!applicationReadsPaused && !closed.get() && automaticRead) {
			channel.config().setAutoRead(true);
			channel.read();
		}
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
		CompletableFuture<Void> prepare = prepareResult.get();
		if (prepare != null) prepare.completeExceptionally(failure);
		CompletableFuture<Void> commit = commitResult.get();
		if (commit != null) commit.completeExceptionally(failure);
		Runnable cleanup = () -> {
			failQueued(failure);
			pendingReads.clear();
			readDeliveryInFlight = false;
			channel.close();
		};
		channel.eventLoop().execute(cleanup);
		callbackExecutor.execute(() -> handler.onClosed(failure));
	}

	private void failQueued(HolepunchFailure failure) {
		OutboundRequest request;
		while ((request = writes.pollFirst()) != null) {
			if (request.kind() == Kind.DATA) {
				pendingBytes.addAndGet(-request.size());
			}
			request.result().completeExceptionally(failure);
		}
	}

	public static final class RawInboundHandler extends ChannelInboundHandlerAdapter {
		private final NettyHolepunchConnection connection;

		public RawInboundHandler(NettyHolepunchConnection connection) {
			this.connection = connection;
		}

		@Override
		public void channelRead(ChannelHandlerContext context, Object message) {
			try {
				ByteBuf input;
				boolean rawEra;
				if (message instanceof RawInbound raw) {
					input = raw.content();
					rawEra = true;
				} else if (message instanceof ByteBuf buf) {
					input = buf;
					rawEra = false;
				} else {
					context.fireChannelRead(message);
					return;
				}
				byte[] bytes = new byte[input.readableBytes()];
				input.readBytes(bytes);
				long total = connection.receivedBytes.addAndGet(bytes.length);
				DebugLog.log("SERVER", connection.debugId, "%s inbound bytes=%d totalReceived=%d %s", rawEra ? "raw" : "framed", bytes.length, total, DebugLog.preview(bytes));
				ByteBuffer buffer = ByteBuffer.wrap(bytes).asReadOnlyBuffer();
				if (rawEra) {
					connection.dispatchRawRead(buffer);
				} else {
					connection.dispatchRead(buffer);
				}
			} finally {
				ReferenceCountUtil.release(message);
			}
		}

		@Override
		public void channelReadComplete(ChannelHandlerContext context) {
			DebugLog.log("SERVER", connection.debugId, "channelReadComplete active=%s writable=%s autoRead=%s", context.channel().isActive(), context.channel().isWritable(), context.channel().config().isAutoRead());
			context.fireChannelReadComplete();
		}

		@Override
		public void channelWritabilityChanged(ChannelHandlerContext context) {
			DebugLog.log("SERVER", connection.debugId, "channelWritabilityChanged writable=%s beforeWritable=%d beforeUnwritable=%d", context.channel().isWritable(), context.channel().bytesBeforeWritable(),
					context.channel().bytesBeforeUnwritable());
			context.fireChannelWritabilityChanged();
		}

		@Override
		public void channelInactive(ChannelHandlerContext context) {
			DebugLog.log("SERVER", connection.debugId, "channelInactive");
			connection.closeWith(new HolepunchFailure(HolepunchFailure.Kind.CLOSED, "channel closed", null));
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
			DebugLog.failure("SERVER", connection.debugId, "pipeline exception", cause);
			connection.closeWith(new HolepunchFailure(HolepunchFailure.Kind.IO, "channel failed", cause));
		}
	}

	private String debugState() {
		return String.format(java.util.Locale.ROOT,
				"active=%s open=%s registered=%s writable=%s autoRead=%s applicationReadsPaused=%s readDeliveryInFlight=%s phase=%s raw=%s pending=%d queue=%d writing=%s sent=%d received=%d "
						+ "beforeWritable=%d beforeUnwritable=%d pipeline=%s",
				channel.isActive(),
				channel.isOpen(), channel.isRegistered(), channel.isWritable(), channel.config().isAutoRead(), applicationReadsPaused, readDeliveryInFlight, handoff.phase(), encryption != null && encryption.isRaw(),
				pendingBytes.get(), writes.size(), writing,
				sentBytes.get(),
				receivedBytes.get(), channel.bytesBeforeWritable(),
				channel.bytesBeforeUnwritable(), channel.pipeline().names());
	}

	private enum Kind {
		DATA,
		READY,
		ACK,
		COMMIT
	}

	private record OutboundRequest(ByteBuffer data, int size, CompletableFuture<Void> result, boolean rawEra, Kind kind) {}

	private record PendingRead(ByteBuffer data, boolean rawEra) {}

	/** Marks bytes read from the raw post-handoff stream so the inbound handler can route them to {@code onRawRead}. */
	record RawInbound(ByteBuf content) {}
}
