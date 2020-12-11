package javaCode.data;

import com.wrapper.spotify.model_objects.specification.AudioFeatures;
import org.apache.commons.io.FileUtils;
import org.json.*;
import java.io.*;
import java.util.*;
import java.io.IOException;

/**
 * Converts the given json file slices in the MPD into two files: adj list style playlists, lookup table of audio feats.
 */
public class LocalDataProcessor {

    /**
     * Driver function to read json input slices, generate playlists.txt & songs.txt
     */
    public static void main(String[] args) throws IOException {
        // Setup that could be taken as args later on
        String inputFile = "input_json/";
        String playlistsFile = "input_processed/playlists.txt";
        String songsFile = "input_processed/songs.txt";
        String failedQueriesFile = "input_processed/failed_queries.txt";
        String encoding = "utf-8";
        String delim = "\t";
        int offsetTrackUri = "spotify:track:".length();
        String clientID = "e840ce30f1184e6999971e37ef35f5ff";
        String clientSecret = "195d5c34b6f242f7afaa1e1022eac024";

        // Parse through the json input slices, write out playlists.txt, get back hashmap of unique songs
        HashMap<String, List<String>> songMap = parseJsonInputSlices(inputFile, playlistsFile, encoding, delim, offsetTrackUri);

        // Use the spotify api to get song data
        HashMap<String, List<String>> failedQueries = new HashMap<>();
        try {
            SpotifyData spotify = new SpotifyData(clientID, clientSecret);
            failedQueries = querySongData(spotify, songMap, songsFile, delim);
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
        System.out.println("Song Query had " + failedQueries.size() + " failures.");

        // Write the failed queries to file so they are not lost & something can be done about them later...
        saveSongMap(failedQueries, failedQueriesFile, delim);
    }

    /**
     * Reads through the input directory and parses out the json files.
     * Writes out an adj list style playlists file: pid \t song,song,song,...
     * Returns a HashMap of the unique songs encountered across all files
     */
    private static HashMap<String, List<String>> parseJsonInputSlices(String inputFile, String playlistsFile, String encoding,
                                                                      String delim, int offsetTrackUri) throws IOException {
        // Gather a list of all the MPD slices in the input directory
        String[] paths = new File(inputFile).list();
        if (paths == null) {
            throw new IOException("No files found in the input directory.");
        }

        // Prepare to append data to the same playlists file many times
        BufferedWriter writer = new BufferedWriter(new FileWriter(playlistsFile));

        // Keep a record of all the unique songs encountered to use for getting audio features
        // key: track_uri (song id) as string & val: List of 2 Strings (track_name, artist_name)
        HashMap<String, List<String>> songMap = new HashMap<String, List<String>>();

        // For each file in the input directory
        for (int i = 0; i < paths.length; i++) {
            // Read the whole file into a giant json object (this is the given format, ~33mb each file)
            System.out.println("--- Reading File " + (i+1) + "/" + paths.length + " ---");
            String readFile = inputFile + paths[i];
            String data = FileUtils.readFileToString(new File(readFile), encoding);
            JSONObject jsonFile = new JSONObject(data);
            JSONArray playlists = new JSONArray(jsonFile.get("playlists").toString());

            // For each playlist found in this file. There should be 1000 playlists per file
            for (int j = 0; j < playlists.length(); j++) {
                StringBuilder str = new StringBuilder();
                JSONObject inner = new JSONObject(playlists.get(j).toString());
                str.append(inner.get("pid").toString());
                str.append(delim);
                JSONArray tracks = new JSONArray(inner.get("tracks").toString());

                // For each track found in this playlist. The number of tracks will be highly variable
                System.out.println("\tPlaylist " + (j+1) + "/" + playlists.length() + " has " + tracks.length() + " tracks");
                for (int k = 0; k < tracks.length(); k++) {
                    JSONObject song = new JSONObject(tracks.get(k).toString());
                    String id = song.get("track_uri").toString().substring(offsetTrackUri);
                    List<String> readable = new ArrayList<String>();
                    readable.add(song.get("track_name").toString());
                    readable.add(song.get("artist_name").toString());
                    // Note: java language must = 8 for this to work
                    songMap.putIfAbsent(id, readable);
                    str.append(id);
                    str.append(",");
                }

                // Drop the trailing comma, finish off with newline, write to the end of existing playlists.txt file
                System.out.println("\tWriting " + paths[i] + " processed playlists to " + playlistsFile);
                str.setLength(str.length() - 1);
                str.append("\n");
                writer.write(str.toString());
                System.out.println("\t>>> Done");
            }
            System.out.println("\n");
        }
        // Close writer to playlists file, return the set of unique songs
        writer.close();
        return songMap;
    }

    private static HashMap<String, List<String>> querySongData(SpotifyData spotify, HashMap<String, List<String>> songMap,
                                                               String songsFile, String delim) throws IOException {
        // Counting from 1 to make modulo calculation clearer, spotify api wants list of a max of 100 songs at a time!
        int counter = 1;
        int queryChunk = 100;
        int writeFrequency = 100;
        int uniqueCount = songMap.size();

        // Prepare to append data to the same playlists file many times
        BufferedWriter writer = new BufferedWriter(new FileWriter(songsFile));
        HashMap<String, List<String>> failedQueries = new HashMap<>();
        StringBuilder songTableBuffer = new StringBuilder();
        List<List<String>> songQueryList = new ArrayList<>(queryChunk);

        // For each (unique) song recorded in the set
        Iterator<Map.Entry<String,List<String>>> itr = songMap.entrySet().iterator();
        System.out.println("--- Starting song queries for " + uniqueCount + " unique songs ---");
        while (itr.hasNext()) {

            // Query song features, write successes to file
            Map.Entry<String,List<String>> entry = itr.next();
            try {
                // Always add the song id to the query list (key, song name, artist name)
                List<String> currentSong = new ArrayList<>(3);
                currentSong.add(entry.getKey());
                currentSong.add(entry.getValue().get(0));
                currentSong.add(entry.getValue().get(1));
                songQueryList.add(currentSong);

                // Query audio features in chunks of 100 songs
                if (counter % queryChunk == 0) {
                    // Get a string array of the keys
                    String[] ids = new String[queryChunk];
                    for (int i = 0; i < songQueryList.size(); i++) {
                        ids[i] = songQueryList.get(i).get(0);
                    }
                    // Objects returned in json format in the order requested
                    AudioFeatures[] features = spotify.getAudioFeaturesList(ids);
                    // Add all the song features line by line
                    for (int i = 0; i < songQueryList.size(); i++) {
                        List<String> data = songQueryList.get(i);
                        songTableBuffer.append(songLine(data.get(0), data.get(1), data.get(2), features[i], delim));
                    }
                    // Reset the song query list
                    System.out.println("\t[" + (counter/uniqueCount) + "%] Queried chunk " + (counter-100) + " - " + counter + " of total " + uniqueCount);
                    songQueryList = new ArrayList<>(queryChunk);
                }


                // Periodically write out the string builder to avoid out of memory but balance frequency of writes
                if (counter % writeFrequency == 0) {
                    System.out.println("\tWriting chunk " + (counter-writeFrequency) + " - " + counter + " to file");
                    writer.write(songTableBuffer.toString());
                    songTableBuffer = new StringBuilder();
                    System.out.println("\t>>>Done");
                }
            }

            // Keep track & report failed attempts
            catch (Exception e) {
                // Report failure to terminal
                System.out.println("\tQUERY FAILED on chunk " + (counter-queryChunk) + " to " + counter);
                System.out.println(e.getMessage());
                // For |queryChunk| songs in the query list, save songs for later processing because of api exception
                for (int i = 0; i < songQueryList.size(); i++) {
                    List<String> data = songQueryList.get(i);
                    String id = data.get(0);
                    List<String> value = new ArrayList<>(2);
                    value.add(data.get(1));
                    value.add(data.get(2));
                    failedQueries.put(id, value);
                }
                // Reset the song query list and continue
                songQueryList = new ArrayList<>(queryChunk);
            }
            counter++;
        }

        // Clear out any remaining song data
        if (songTableBuffer.length() != 0) {
            System.out.println("\tWriting final remaining chunk to file");
            writer.write(songTableBuffer.toString());
            System.out.println("\t>>>Done");
        }

        // Close writer to songs file, return set of failed song features queries
        writer.close();
        return failedQueries;
    }

    /**
     * Creates a consistent representation for song data.
     * - Key: songID
     * - Val: title,artist,features in the order they appear here: https://developer.spotify.com/documentation/web-api/reference/tracks/get-audio-features/
     */
    private static String songLine(String songID, String title, String artist, AudioFeatures features, String delim) {
        // Features in order of the api website
        // Chained appends are more efficient than string concatenation
        StringBuilder str = new StringBuilder();
        str.append(songID);
        str.append(delim);
        str.append(title);
        str.append(",");
        str.append(artist);
        str.append(",");
        str.append(features.getDurationMs());
        str.append(",");
        str.append(features.getKey());
        str.append(",");
        str.append(features.getMode());
        str.append(",");
        str.append(features.getTimeSignature());
        str.append(",");
        str.append(features.getAcousticness());
        str.append(",");
        str.append(features.getDanceability());
        str.append(",");
        str.append(features.getEnergy());
        str.append(",");
        str.append(features.getInstrumentalness());
        str.append(",");
        str.append(features.getLiveness());
        str.append(",");
        str.append(features.getLoudness());
        str.append(",");
        str.append(features.getSpeechiness());
        str.append(",");
        str.append(features.getValence());
        str.append(",");
        str.append(features.getTempo());
        str.append("\n");
        return str.toString();
    }

    /**
     * Writes the songMap to file, for instance to save the entries for which the api query failed.
     */
    private static void saveSongMap(HashMap<String,List<String>> songMap, String failedQueriesFile, String delim) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(failedQueriesFile));
        StringBuilder str = new StringBuilder();
        // For the entries in the hashmap
        for (Map.Entry<String,List<String>> entry : songMap.entrySet()) {
            str.append(entry.getKey());
            str.append(delim);
            str.append(entry.getValue().get(0));
            str.append(",");
            str.append(entry.getValue().get(1));
            str.append("\n");
        }
        writer.write(str.toString());
        writer.close();

    }}
