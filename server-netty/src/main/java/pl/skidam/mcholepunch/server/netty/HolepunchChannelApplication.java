package pl.skidam.mcholepunch.server.netty;

import io.netty.channel.Channel;

import pl.skidam.mcholepunch.HolepunchConnection;

/** Receives a taken-over holepunch channel with the transport already installed. */
public interface HolepunchChannelApplication {

	/**
	 * Called once on the event loop after the takeover, before any application byte flows.
	 * Inbound application bytes arrive as decrypted transport-frame payloads before the handoff
	 * and as raw pass-through bytes after it; outbound application writes go through the
	 * pipeline toward the transport framer.
	 */
	void install(Channel channel, HolepunchConnection connection) throws Exception;
}
