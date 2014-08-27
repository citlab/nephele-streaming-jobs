package de.tuberlin.cit.test.task;

import de.tuberlin.cit.test.record.StringRecord;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractGenericInputTask;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class SimpleNetworkStreamSourceTask extends AbstractGenericInputTask {
	private RecordWriter<StringRecord> out;
	private BufferedReader in;
	public static final String TCP_SERVER_PORT = "simplenetworkstreamsourcetask.tcpserverport";
	public static final int DEFAULT_TCP_SERVER_PORT = 9000;

	@Override
	public void registerInputOutput() {
		out = new RecordWriter<StringRecord>(this, StringRecord.class);
	}

	@Override
	public void invoke() throws Exception {
		ServerSocket serverSocket = null;
		Socket socket = null;

		if (in == null) {
			try {
				serverSocket = new ServerSocket(
						this.getTaskConfiguration().getInteger(TCP_SERVER_PORT, DEFAULT_TCP_SERVER_PORT));
				socket = serverSocket.accept();
				in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		String line;
		while ((line = in.readLine()) != null) {
			out.emit(new StringRecord(line));
		}
		out.flush();
		in.close();
		if (socket != null) {
			socket.close();
		}
		if (serverSocket != null) {
			serverSocket.close();
		}
	}
}
