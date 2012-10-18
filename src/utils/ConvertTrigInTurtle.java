package utils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;

public class ConvertTrigInTurtle {

	public static long LIMIT_FILE = 100000000; // Split the files after 50MB

	public static void main(String[] args) {
		System.out.println("Process file: " + args[0]);

		try {
			FileReader fileIn = new FileReader(args[0]);
			BufferedReader reader = new BufferedReader(fileIn);
			int i = 0;
			FileOutputStream fileOut = new FileOutputStream(new File(args[0]
					+ "." + i + ".turtle"));
			BufferedOutputStream out = new BufferedOutputStream(fileOut);

			String line = null;
			long sizeFile = 0;
			while ((line = reader.readLine()) != null) {
				if (!line.startsWith("}") && !line.endsWith("{")) {
					byte[] bLine = line.getBytes();
					sizeFile += bLine.length;

					if (sizeFile > LIMIT_FILE && line.endsWith(".")) { // End of
						// statement
						out.flush();
						out.close();
						fileOut.flush();
						fileOut.close();
						sizeFile = 0;
						++i;
						fileOut = new FileOutputStream(new File(args[0] + "."
								+ i + ".turtle"));
						out = new BufferedOutputStream(fileOut);
					}
					out.write(line.getBytes());
					out.write('\n');
				}
			}

			out.flush();
			out.close();
			fileOut.flush();
			fileOut.close();
			reader.close();

			System.out.println("Conversion ok!");
		} catch (Exception e) {
			System.err.println(e);
		}
	}

}
