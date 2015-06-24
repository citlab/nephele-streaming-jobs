package de.tuberlin.cit.livescale.job.util.encoder;

import java.util.HashSet;

public class ServerPortManager {

	private HashSet<Integer> allocatedPorts;

	private final int portRangeBegin;

	private final int portRangeEnd;

	public ServerPortManager(int portRangeBegin, int portRangeEnd) {
		this.portRangeBegin = portRangeBegin;
		this.portRangeEnd = portRangeEnd;
		this.allocatedPorts = new HashSet<Integer>();
	}

	public void releaseAllocatedPort(int allocatedPort) {
		if (allocatedPort != -1) {
			synchronized (allocatedPorts) {
				allocatedPorts.remove(allocatedPort);
			}
		}
	}

	public int bindToFreePort(PortBinder portBinder) throws Exception {
		int allocatedPort = -1;

		synchronized (allocatedPorts) {
			for (int currPort = portRangeBegin; currPort <= portRangeEnd; currPort++) {
				if (!allocatedPorts.contains(currPort)) {
					try {
						portBinder.attemptToBind(currPort);
						allocatedPorts.add(currPort);
						allocatedPort = currPort;
						break;
					} catch (Exception e) {
					}
				}
			}
		}
		if (allocatedPort == -1) {
			throw new Exception(
				String.format(
					"Could not bind to any port. Port range - %d-%d is used up.", portRangeBegin,
					portRangeEnd));
		} else {
			return allocatedPort;
		}
	}
}
