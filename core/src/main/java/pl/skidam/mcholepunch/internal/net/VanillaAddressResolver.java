package pl.skidam.mcholepunch.internal.net;

import java.net.*;
import java.util.Hashtable;

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
		return new HolepunchRoute(normalizedHost, port, new InetSocketAddress(address, selectedPort));
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
			String[] fields = attribute.get(0).toString().trim().split("\\s+");
			if (fields.length != 4) {
				return null;
			}
			int port = Integer.parseInt(fields[2]);
			String target = fields[3];
			if (SERVICE_UNAVAILABLE_TARGET.equals(target)) {
				return new SrvRecord(target, port);
			}
			if (target.endsWith(".")) {
				target = target.substring(0, target.length() - 1);
			}
			if (target.isBlank() || port < 1 || port > 65535) {
				return null;
			}
			return new SrvRecord(target, port);
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

	public record SrvRecord(String host, int port) {}
}
