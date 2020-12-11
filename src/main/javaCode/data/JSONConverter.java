package javaCode.data;

import com.wrapper.spotify.model_objects.specification.AudioFeatures;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Converts the given json file slices in the MPD into two files: adj list style playlists, lookup table of audio feats.
 * Supports static utility functions for use during data generation phase.
 *
 * Ex:
 * HashMap map = JSONConverter.parseInputSlice(args)
 */
public class JSONConverter {

    final static String encoding = "utf-8";
    final static String delim = "\t";
    final static String sep = ",";
    final static int offsetTrackUri = "spotify:track:".length();

    /**
     * Extracts locally unique songs into a hashmap.
     *
     * @param line an entire mpd.slice.json file as a string on one line
     * @return HashMap of locally unique songs as (id, (title, artist))
     */
    public static HashMap<String, List<String>> parseInputSliceForSongs(String line) {
        // Entire json file stored in the input line
        JSONObject jsonFile = new JSONObject(line);
        JSONArray playlists = new JSONArray(jsonFile.get("playlists").toString());
        HashMap<String, List<String>> songMap = new HashMap<String, List<String>>();

        // For each playlist found in the file. There should be 1000 playlists per file.
        for (int i = 0; i < playlists.length(); i++) {
            JSONObject inner = new JSONObject(playlists.get(i).toString());
            JSONArray tracks = new JSONArray(inner.get("tracks").toString());

            // For each song found in the playlist. The number of tracks will be highly variable.
            for (int j = 0; j < tracks.length(); j++) {
                JSONObject song = new JSONObject(tracks.get(j).toString());
                String id = song.get("track_uri").toString().substring(offsetTrackUri);
                List<String> readable = new ArrayList<String>();
                readable.add(song.get("track_name").toString());
                readable.add(song.get("artist_name").toString());
                songMap.putIfAbsent(id, readable);
            }
        }
        return songMap;
    }

    /**
     * Extracts playlists into a hashmap.
     *
     * @param line an entire mpd.slice.json file as a string on one line
     * @return HashMap of playlists as (pid, (song list))
     */
    public static HashMap<String,String> parseInputSliceForPlaylists(String line) {
        // Entire json file stored in the input line
        JSONObject jsonFile = new JSONObject(line);
        JSONArray playlists = new JSONArray(jsonFile.get("playlists").toString());
        HashMap<String,String> playlistMap = new HashMap<>();

        // For each playlist found in the file. There should be 1000 playlists per file.
        for (int i = 0; i < playlists.length(); i++) {
            JSONObject inner = new JSONObject(playlists.get(i).toString());
            String pid = inner.get("pid").toString();
            JSONArray tracks = new JSONArray(inner.get("tracks").toString());
            StringBuilder trackList = new StringBuilder();

            // For each song in the playlist. The number of tracks will be highly variable.
            for (int j = 0; j < tracks.length(); j++) {
                JSONObject song = new JSONObject(tracks.get(j).toString());
                trackList.append(song.get("track_uri").toString().substring(offsetTrackUri));
                trackList.append(sep);
            }

            // Drop the trailing comma and add the playlist to the HashMap
            trackList.setLength(trackList.length()-1);
            playlistMap.put(pid, trackList.toString());
        }
        return playlistMap;
    }

    public static String songFeatures(String title, String artist, AudioFeatures features) {
        // Remove any commas from title and artist
        title = title.replace(",","");
        artist = artist.replace(",","");

        // Features in order of the api website
        // Chained appends are more efficient than string concatenation
        StringBuilder str = new StringBuilder();
        str.append(title);
        str.append(sep);
        str.append(artist);
        str.append(sep);
        str.append(features.getDurationMs());
        str.append(sep);
        str.append(features.getKey());
        str.append(sep);
        String mode = features.getMode().toString();
        // Handle encoding for major/minor
        if (mode.equals("MAJOR")) {
            mode = "1";
        }
        else{
            mode = "0";
        }
        str.append(mode);
        str.append(sep);
        str.append(features.getTimeSignature());
        str.append(sep);
        str.append(features.getAcousticness());
        str.append(sep);
        str.append(features.getDanceability());
        str.append(sep);
        str.append(features.getEnergy());
        str.append(sep);
        str.append(features.getInstrumentalness());
        str.append(sep);
        str.append(features.getLiveness());
        str.append(sep);
        str.append(features.getLoudness());
        str.append(sep);
        str.append(features.getSpeechiness());
        str.append(sep);
        str.append(features.getValence());
        str.append(sep);
        str.append(features.getTempo());
        return str.toString();
    }

}
