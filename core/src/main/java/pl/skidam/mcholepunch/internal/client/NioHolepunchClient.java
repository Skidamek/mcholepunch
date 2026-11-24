package pl.skidam.mcholepunch.internal.client;

import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import pl.skidam.mcholepunch.HolepunchConnection;
import pl.skidam.mcholepunch.HolepunchFailure;
import pl.skidam.mcholepunch.HolepunchRoute;
import pl.skidam.mcholepunch.internal.DebugLog;
import pl.skidam.mcholepunch.internal.HolepunchRequest;
import pl.skidam.mcholepunch.internal.net.VanillaAddressResolver;

public final class NioHolepunchClient {
	/**
	 * Bounds concurrent connect work: each attempt holds a thread for the whole negotiation, so an
	 * unbounded pool would let stalled networks grow the thread count. This client is one
	 * connection per game client; four threads cover retries, and excess requests queue behind
	 * them. Resolve runs on its own small pool so a caller who blocks inside a resolve callback on
	 * a connect future cannot starve the connects of their threads.
	 */
	private static final ExecutorService CONNECT_EXECUTOR = Executors.newFixedThreadPool(4, runnable -> {
		Thread thread = new Thread(runnable, "mcholepunch-connect");
		thread.setDaemon(true);
		return thread;
	});
	private static final ExecutorService RESOLVE_EXECUTOR = Executors.newFixedThreadPool(2, runnable -> {
		Thread thread = new Thread(runnable, "mcholepunch-resolve");
		thread.setDaemon(true);
		return thread;
	});

	private final VanillaAddressResolver resolver;

	public NioHolepunchClient() {
		this(new VanillaAddressResolver());
	}

	NioHolepunchClient(VanillaAddressResolver resolver) {
		this.resolver = resolver;
	}

	public CompletionStage<HolepunchRoute> resolve(String host, int port) {
		return CompletableFuture.supplyAsync(() -> {
			try {
				return resolver.resolve(host, port);
			} catch (Exception e) {
				throw new CompletionException(e);
			}
		}, RESOLVE_EXECUTOR);
	}

	public CompletionStage<HolepunchConnection> connect(HolepunchRequest request) {
		CompletableFuture<HolepunchConnection> result = new CompletableFuture<>();
		long debugId = DebugLog.nextConnectionId();
		DebugLog.log("CLIENT", debugId, "connect requested host=%s port=%d protocol=%d", request.route().handshakeHost(), request.route().handshakePort(), request.protocolVersion());
		CONNECT_EXECUTOR.execute(() -> {
			SocketChannel channel = null;
			try {
				HolepunchRoute route = request.route();
				DebugLog.log("CLIENT", debugId, "resolved address host=%s port=%d socket=%s", route.handshakeHost(), route.handshakePort(), route.transportAddress());
				channel = SocketChannel.open();
				channel.configureBlocking(true);
				channel.socket().setTcpNoDelay(true);
				channel.socket().setKeepAlive(true);
				channel.socket().connect(route.transportAddress(), Math.toIntExact(request.options().connectTimeout().toMillis()));
				DebugLog.log("CLIENT", debugId, "TCP connected local=%s remote=%s", channel.getLocalAddress(), channel.getRemoteAddress());
				NioHolepunchConnection connection = NioHolepunchConnection.negotiate(debugId, channel, request, route);
				channel = null;
				DebugLog.log("CLIENT", debugId, "connect completed");
				result.complete(connection);
			} catch (Throwable throwable) {
				HolepunchFailure failure = throwable instanceof HolepunchFailure holepunchFailure ? holepunchFailure : new HolepunchFailure(HolepunchFailure.Kind.IO, "connection failed", throwable);
				DebugLog.failure("CLIENT", debugId, "connect failed", failure);
				if (channel != null) {
					try {
						channel.close();
					} catch (Exception ignored) {
					}
				}
				result.completeExceptionally(failure);
			}
		});
		return result;
	}
}
