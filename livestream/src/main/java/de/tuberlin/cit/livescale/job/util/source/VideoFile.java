package de.tuberlin.cit.livescale.job.util.source;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class VideoFile {

	private String path;

	private byte[] data;

	public VideoFile(String path) {
		this.path = path;
	}

	public void loadIntoMemory() throws IOException {
		File file = new File(this.path);

		if (!file.canRead()) {
			throw new IOException("Cannot read file " + this.path);
		}

		this.data = new byte[(int) file.length()];
		BufferedInputStream in = new BufferedInputStream(new FileInputStream(this.path));

		int read = 0;

		while (read < file.length()) {
			read += in.read(data, read, data.length - read);
		}
		in.close();
	}

	public String getPath() {
		return path;
	}

	public byte[] getData() {
		return data;
	}
}
