package pl.skidam.mcholepunch.internal.net;

import java.net.*;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.InitialDirContext;

import pl.skidam.mcholepunch.HolepunchRoute;

public class VanillaAddressResolver {
	private static final int DEFAULT_MINECRAFT_PORT = 25565;
	private static final String SERVICE_UNAVAILABLE_TARGET = ".";

	public HolepunchRoute resolve(String host, int port) throws UnknownHostException {
		if (host == null || host.isBlank()) {
			throw new UnknownHostException("Host is blank");
		}
		if (port < 1 || port > 65535) {
			throw new IllegalArgumentException("Invalid port: " + port);
		}

		String normalizedHost = host.strip();

		boolean startsWithBracket = normalizedHost.startsWith("[");
		boolean endsWithBracket = normalizedHost.endsWith("]");

		if (startsWithBracket != endsWithBracket) {
			throw new UnknownHostException("Malformed bracketed host: " + normalizedHost);
		}

		boolean literalHost = isIpLiteral(normalizedHost);

		if (startsWithBracket && (!literalHost || normalizedHost.indexOf(':') < 0)) {
			throw new UnknownHostException("Bracketed host is not a valid IPv6 literal: " + normalizedHost);
		}

		String selectedHost = normalizedHost;
		int selectedPort = port;
		if (port == DEFAULT_MINECRAFT_PORT && !literalHost) {
			SrvRecord record = lookupSrv(normalizedHost);
			if (record != null) {
				if (SERVICE_UNAVAILABLE_TARGET.equals(record.host())) {
					throw new UnknownHostException("Minecraft service is unavailable for " + normalizedHost);
				}
				selectedHost = record.host();
				selectedPort = record.port();
			}
		}
		InetAddress address = InetAddress.getByName(selectedHost);
		if (literalHost) {
			address = patchLiteralAddress(address);
		}
		return HolepunchRoute.at(normalizedHost, port, new InetSocketAddress(address, selectedPort));
	}

	private static InetAddress patchLiteralAddress(InetAddress address) throws UnknownHostException {
		String literalHostName = address.getHostAddress();
		if (address instanceof Inet6Address ipv6Address) {
			NetworkInterface scopedInterface = ipv6Address.getScopedInterface();
			if (scopedInterface != null) {
				return Inet6Address.getByAddress(literalHostName, ipv6Address.getAddress(), scopedInterface);
			}
			int scopeId = ipv6Address.getScopeId();
			if (scopeId != 0) {
				return Inet6Address.getByAddress(literalHostName, ipv6Address.getAddress(), scopeId);
			}
		}
		return InetAddress.getByAddress(literalHostName, address.getAddress());
	}

	private static boolean isIpLiteral(String host) {
		String candidate = host;
		if (candidate.startsWith("[") && candidate.endsWith("]")) {
			candidate = candidate.substring(1, candidate.length() - 1);
		}
		if (isIpv4Literal(candidate)) {
			return true;
		}
		if (candidate.indexOf(':') < 0) {
			return false;
		}
		try {
			InetAddress.getByName(candidate);
			return true;
		} catch (UnknownHostException exception) {
			return false;
		}
	}

	private static boolean isIpv4Literal(String candidate) {
		String[] parts = candidate.split("\\.", -1);
		if (parts.length != 4) {
			return false;
		}
		for (String part : parts) {
			if (!part.matches("\\d{1,3}")) {
				return false;
			}
			if (Integer.parseInt(part) > 255) {
				return false;
			}
		}
		return true;
	}

	protected SrvRecord lookupSrv(String host) {
		Hashtable<String, String> environment = new Hashtable<>();
		environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
		environment.put(Context.PROVIDER_URL, "dns:");
		environment.put("com.sun.jndi.dns.timeout.retries", "1");
		InitialDirContext context = null;
		try {
			context = new InitialDirContext(environment);
			Attribute attribute = context.getAttributes("_minecraft._tcp." + host, new String[]{"SRV"}).get("SRV");
			if (attribute == null || attribute.size() == 0) {
				return null;
			}
			List<String> rawRecords = new ArrayList<>(attribute.size());
			for (int index = 0; index < attribute.size(); index++) {
				Object value = attribute.get(index);
				if (value != null) {
					rawRecords.add(value.toString());
				}
			}
			return selectSrvRecord(rawRecords);
		} catch (NamingException | RuntimeException exception) {
			return null;
		} finally {
			if (context != null) {
				try {
					context.close();
				} catch (NamingException | RuntimeException ignored) {
				}
			}
		}
	}

	/** Selects one SRV target using RFC 2782 priority (lowest first) and weight ordering. */
	static SrvRecord selectSrvRecord(List<String> rawRecords) {
		int bestPriority = Integer.MAX_VALUE;
		List<WeightedSrv> candidates = new ArrayList<>();
		for (String raw : rawRecords) {
			WeightedSrv parsed = parseSrvRecord(raw);
			if (parsed == null) {
				continue;
			}
			if (parsed.priority() < bestPriority) {
				bestPriority = parsed.priority();
				candidates.clear();
				candidates.add(parsed);
			} else if (parsed.priority() == bestPriority) {
				candidates.add(parsed);
			}
		}
		if (candidates.isEmpty()) {
			return null;
		}
		if (candidates.size() == 1) {
			return candidates.get(0).record();
		}
		long totalWeight = 0;
		for (WeightedSrv candidate : candidates) {
			totalWeight += candidate.weight();
		}
		if (totalWeight <= 0) {
			return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size())).record();
		}
		long pick = ThreadLocalRandom.current().nextLong(totalWeight);
		long running = 0;
		for (WeightedSrv candidate : candidates) {
			if (candidate.weight() == 0) {
				continue;
			}
			running += candidate.weight();
			if (pick < running) {
				return candidate.record();
			}
		}
		return candidates.get(candidates.size() - 1).record();
	}

	private static WeightedSrv parseSrvRecord(String raw) {
		if (raw == null) {
			return null;
		}
		String[] fields = raw.trim().split("\\s+");
		if (fields.length != 4) {
			return null;
		}
		try {
			int priority = Integer.parseInt(fields[0]);
			int weight = Integer.parseInt(fields[1]);
			int port = Integer.parseInt(fields[2]);
			String target = fields[3];
			if (priority < 0 || weight < 0) {
				return null;
			}
			if (SERVICE_UNAVAILABLE_TARGET.equals(target)) {
				return new WeightedSrv(priority, weight, new SrvRecord(target, port));
			}
			if (target.endsWith(".")) {
				target = target.substring(0, target.length() - 1);
			}
			if (target.isBlank() || port < 1 || port > 65535) {
				return null;
			}
			return new WeightedSrv(priority, weight, new SrvRecord(target, port));
		} catch (NumberFormatException exception) {
			return null;
		}
	}

	private record WeightedSrv(int priority, int weight, SrvRecord record) {}

	public record SrvRecord(String host, int port) {}
}
