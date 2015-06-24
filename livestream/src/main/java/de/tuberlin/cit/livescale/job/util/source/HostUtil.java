package de.tuberlin.cit.livescale.job.util.source;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.LinkedList;

public class HostUtil {

	/**
	 * Provides "the" IP address of the underlying host. Since any host can have multiple network
	 * interfaces with multiple IPs each, this implementation prefers internet-routable IPs over private IPs.
	 * If multiple internet-routable IPs or no internet-routable IPs but multiple
	 * private IPs are available, it chooses one randomly.
	 * 
	 * @return A String representation of the chosen IP.
	 * @throws SocketException
	 */
	public static String determineHostAddress() throws SocketException {

		LinkedList<Inet4Address> candidateAddresses = new LinkedList<Inet4Address>();

		Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
		while (netInterfaces.hasMoreElements()) {
			NetworkInterface ni = (NetworkInterface) netInterfaces.nextElement();
			Enumeration<InetAddress> interfaceAddresses = ni.getInetAddresses();

			while (interfaceAddresses.hasMoreElements()) {
				InetAddress ip = interfaceAddresses.nextElement();
				if (!ip.isLoopbackAddress() && ip instanceof Inet4Address) {
					candidateAddresses.add((Inet4Address) ip);
				}
			}
		}

		String chosenAddress = null;

		for (Inet4Address ip : candidateAddresses) {
			if (!isPrivateIp(ip) || chosenAddress == null) {
				chosenAddress = ip.getHostAddress();
			}
		}

		return chosenAddress;
	}

	private static boolean isPrivateIp(Inet4Address ip) {
		byte[] ipBytes = ip.getAddress();

		return ipBytes[0] == 10 ||
			((ipBytes[0] & 0xFF) == 172 && (ipBytes[1] & 0xF0) == 16) ||
			((ipBytes[0] & 0xFF) == 192 && (ipBytes[1] & 0xFF) == 168);
	}
}
