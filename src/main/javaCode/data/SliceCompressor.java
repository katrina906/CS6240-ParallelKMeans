package javaCode.data;

import org.apache.commons.io.FileUtils;

import java.io.*;

/**
 * A class to compress the playlist json slices into single line files. This must be run locally.
 * Allows us to input each playlist as one line of input for Mapper in MRDataProcessor. Otherwise each playlist many lines
 */
public class SliceCompressor {

    public static void main(String[] args) throws IOException {
        String inputFile = args[0];  //"input_json/"
        String localOut = args[1]; //"input_line/line."
        String[] paths = new File(inputFile).list();
        if(paths == null) {
            throw new IOException("No files found in input directory " + inputFile);
        }
        // loop through files in local directory, compress into single line files, and write out locally
        for (int i = 0; i < paths.length; i++) {
            String jsonAsSingleLine = lineify(inputFile + paths[i]);
            save(jsonAsSingleLine, localOut + paths[i]);
            System.out.println("Processed file " + (i+1) + " of " + paths.length);
        }
    }

    /**
     * Removes newlines and outputs the file as one long line.
     */
    private static String lineify(String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(path)));
        StringBuilder jsonAsSingleLine = new StringBuilder();
        // Read line by line and concatenate them
        String line = reader.readLine();
        while(line != null) {
            jsonAsSingleLine.append(line);
            line = reader.readLine();
        }
        return jsonAsSingleLine.toString();
    }

    private static void save(String jsonAsSingleLine, String path) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(path));
        writer.write(jsonAsSingleLine);
        writer.close();
    }
}
