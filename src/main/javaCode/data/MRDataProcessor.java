package javaCode.data;

import java.io.*;
import java.util.*;

import com.wrapper.spotify.exceptions.SpotifyWebApiException;
import com.wrapper.spotify.model_objects.specification.AudioFeatures;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hc.core5.http.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.FileSystem;

public class MRDataProcessor extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(MRDataProcessor.class);

    // JOB 0: Map jsons to (pid, songs list)
    public static class PlaylistMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            HashMap<String,String> playlistMap = JSONConverter.parseInputSliceForPlaylists(value.toString());
            for (Map.Entry<String,String> playlist : playlistMap.entrySet()) {
                context.write(new Text(playlist.getKey()), new Text(playlist.getValue()));
            }
        }
    }

    // JOB 1 Mapper: parse json files and emit each song ID with its name and artist
    // individual jsons have no duplicates (hashmap), but can be duplicates between files from different mappers
    public static class JsonParserMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            // Parse through the json input slices and get back hashmap of unique songs
            HashMap<String, List<String>> songMap = JSONConverter.parseInputSliceForSongs(value.toString());

            // loop through songs in hashmap and emit each one to reduce for duplicate dropping
            for (Map.Entry<String, List<String>> entry : songMap.entrySet()) {
                // write each song id as key with title and artist as value to reducer to drop duplicates
                context.write(new Text(entry.getKey()), new Text(entry.getValue().get(0) + "\t" + entry.getValue().get(1)));
            }
        }
    }

    // JOB 1 Reducer: emit only one instance of each song to drop duplicates
    public static class DuplicateDropReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> value, final Context context) throws IOException, InterruptedException {
            context.write(key, value.iterator().next());
        }
    }

    // JOB 2 Mapper: pass 10 songs to mapper, call API to get numeric features, emit each song
    // Batch calling API so fewer API hits thus faster and less likely to have a throttling problem
    public static class ApiCallMapper extends Mapper<Object, Text, Text, Text> {

        // Class level data structures to collect lists of song ids, titles, and artists
        List<String> idList = new ArrayList<>();
        List<String> titleList = new ArrayList<>();
        List<String> artistList = new ArrayList<>();
        SpotifyData spotify;

        // allow multiple outputs to write success vs failures of API call
        private MultipleOutputs<Text,Text> mos;

        @Override
        public void setup(Context context) {
            // Set up spotify api to get song data
            String clientID = context.getConfiguration().get("clientID");
            String clientSecret = context.getConfiguration().get("clientSecret");
            try {
                spotify = new SpotifyData(clientID, clientSecret);
            } catch (Exception e) {
                e.printStackTrace();
            }

            mos = new MultipleOutputs<>(context);
        }

        @Override
        public void map(final Object key, final Text value, final Context context) {
            // Input value is tab delimited string of form "id, title, artist". Split on \t into list
            String[] song = value.toString().split("\t");

            // add list to class level data structure
            idList.add(song[0]);
            titleList.add(song[1]);
            artistList.add(song[2]);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            // process lists of songs in batches of 100, which is the maximum the Spotify API takes at a time
            // Mapper will have more than 100 songs, so separate into batches
            // More than 100 per Mapper to reduce the number of files written out at the end (1 mpd.json -> 345 song files & there are 1000 mpds)
            int pos = 0;
            int batch = 100;
            while (pos < idList.size()) {
                AudioFeatures[] features = new AudioFeatures[0];
                int end = pos + batch;
                // Avoid going out of bounds when finishing off the list (case when idlist not divisible by 100)
                if (end > idList.size()) {
                    end = idList.size();
                }
                String[] songs = idList.subList(pos, end).toArray(new String[0]);

                // Send the batch of songs to the spotify api
                try {
                    features = spotify.getAudioFeaturesList(songs);
                    // Write out id, title, artist, and audio features line by line. "success" output file.
                    for (int i = 0; i < features.length; i++) {
                        // An audio features object can occasionally be null, write such cases to failure file
                        if (features[i] != null) {
                            mos.write("success", new Text(idList.get(pos)),
                                    new Text(JSONConverter.songFeatures(titleList.get(pos), artistList.get(pos), features[i])));
                        }
                        else {
                            mos.write("failure", new Text(idList.get(i)),
                                    new Text(titleList.get(i) + artistList.get(i)));
                        }
                        pos += 1;
                    }
                }
                // If the whole batch fails, write out songs to "failure" output file
                catch (ParseException | SpotifyWebApiException e) {
                    for (int i = pos; i < pos+batch; i++) {
                        mos.write("failure", new Text(idList.get(i)), new Text(titleList.get(i) + artistList.get(i)));
                        pos += batch;
                    }
                    System.out.println(e);

                }
            }
            mos.close();
        }
    }

    // JOB 3: Keeping only successfully queried songs, append list of the playlists they appear on (as pids)
    // Final output shall be: song_id \t title,artist,audio_features,[pid list separated by ;]
    public static class PlaylistAppearancesMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\t");
            // Failures will have 3 tabs rather than 2, we omit them implicitly here
            if (data.length == 2) {
                String id = data[0];
                // Playlist ids are purely numeric, 0 - 999999
                // We flag "P" for playlists and output all (song_id, playlist_id) pairs. Combiner should be used.
                if (StringUtils.isNumeric(id) && id.length() < 22) {
                    Text pid = new Text("P" + id);
                    String[] songs = data[1].split(",");
                    for (int i = 0; i < songs.length; i++) {
                        context.write(new Text(songs[i]), pid);
                    }
                }
                // Song ids are mixed letters & numbers, and 22 characters long.
                // We flag "A" for audio features and pass along the song data here.
                else {
                    context.write(new Text(id), new Text("A" + data[1]));
                }
            }
        }
    }

    public static class PlaylistAppearancesCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> value, final Context context) throws IOException, InterruptedException {
            StringBuilder playlists = new StringBuilder();
            playlists.append("P");
            for (Text val : value) {
                // Condense playlists, flagged with "P"
                if (val.toString().startsWith("P")) {
                    playlists.append(val.toString().substring(1)).append(";");
                }
                // Just pass along the audio features, flagged with "A"
                else {
                    context.write(key, val);
                }
            }
            // Drop trailing semi colon (found playlists) or delete the initial flag "P" (didn't find any playlists)
            playlists.setLength(playlists.length()-1);
            // If we actually found any playlist ids
            if (playlists.length() > 0) {
                context.write(key, new Text(playlists.toString()));
            }
        }
    }

    public static class PlaylistAppearancesReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(final Text key, final Iterable<Text> value, final Context context) throws IOException, InterruptedException {
            StringBuilder songData = new StringBuilder();
            StringBuilder playlists = new StringBuilder();
            playlists.append(",[");
            for (Text val: value) {
                // Add playlist chunks to the final list of playlist ids (remove the P flag)
                if (val.toString().startsWith("P")) {
                    playlists.append(val.toString().substring(1)).append(";");
                }
                // Keep track of the song data (remove the A flag)
                else {
                    songData.append(val.toString().substring(1));
                }
            }
            // Drop the trailing semi colon & add the closing square bracket
            playlists.setLength(playlists.length()-1);
            playlists.append("]");
            // Append the list of pids to the audio features
            songData.append(playlists);
            // Write out the final data as: song_id \t title,artist,audio_features,[pid;pid;pid;...]
            context.write(key, new Text(songData.toString()));
        }
    }


    // driver method
    @Override
    public int run(final String[] args) throws Exception {
        // Measure runtime
        long start = System.nanoTime();

        // Save args under convenient names
        String inputJsonLines = args[0];
        String intermediate = args[1];
        String outputPlaylists = args[2];
        String outputSongs = args[3];
        String outputSongsWithLists = args[4];
        String clientID = args[5];
        String clientSecret = args[6];

        // Config setup for all jobs
        final Configuration conf = getConf();
        final FileSystem fileSystem = FileSystem.get(conf);

        // -------------------------------------------------------------------------------------------------------------
        // Job 0: Get playlists from the json
        final Job job0 = Job.getInstance(conf, "SpotifyGetPlaylistsWithSongIDs");
        job0.setJarByClass(MRDataProcessor.class);
        final Configuration jobConf0 = job0.getConfiguration();
        jobConf0.set("mapreduce.output.textoutputformat.separator", "\t");
        job0.setMapperClass(PlaylistMapper.class);
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(Text.class);
        job0.setMapOutputKeyClass(Text.class);
        job0.setMapOutputValueClass(Text.class);

        // There are 1000 json files, now compressed to single lines (so line = file), setting 20 map tasks
        job0.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job0, new Path(inputJsonLines));
        job0.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 500);
        FileOutputFormat.setOutputPath(job0, new Path(outputPlaylists));


        // delete output path if it exist (LOCAL DEVELOPMENT ONLY)
        if (fileSystem.exists(new Path(outputPlaylists))) {
            fileSystem.delete(new Path(outputPlaylists), true);
        }

        // run job. If fails, return code
        int job0ResultCode = job0.waitForCompletion(true) ? 0 : 1;
        if (job0ResultCode == 1) {
            return job0ResultCode;
        }
        long job0Time = System.nanoTime();

        // -------------------------------------------------------------------------------------------------------------
        // Job 1: parse playlist JSON and drop duplicate songs between files
        // Read again to keep readable title/artist data while keeping playlists file as compressed as possible (only ids)
        final Job job1 = Job.getInstance(conf, "SpotifyJsonDuplicateDrop");
        job1.setJarByClass(MRDataProcessor.class);
        final Configuration jobConf1 = job1.getConfiguration();
        jobConf1.set("mapreduce.output.textoutputformat.separator", "\t");
        job1.setMapperClass(JsonParserMapper.class);
        job1.setReducerClass(DuplicateDropReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(inputJsonLines));
        FileOutputFormat.setOutputPath(job1, new Path(intermediate));

        // delete output path if it exist (LOCAL DEVELOPMENT ONLY)
        if (fileSystem.exists(new Path(intermediate))) {
            fileSystem.delete(new Path(intermediate), true);
        }
        // run job. If fails, return code
        int job1ResultCode = job1.waitForCompletion(true) ? 0 : 1;
        if (job1ResultCode == 1) {
            return job1ResultCode;
        }
        long job1Time = System.nanoTime();

        // -------------------------------------------------------------------------------------------------------------
        // Job 2: pass 100 songs to each API call to get features (pass 10000 songs to each Mapper and batch 100 within)
        final Job job2 = Job.getInstance(conf, "SpotifyAudioFeatures");
        job2.setJarByClass(MRDataProcessor.class);
        final Configuration jobConf2 = job2.getConfiguration();
        jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");

        job2.setMapperClass(ApiCallMapper.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path(outputSongs));
        // set lines of input per mapper
        job2.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job2, new Path(intermediate));
        job2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 10000);

        // Define two output types: success and failure for API call
        MultipleOutputs.addNamedOutput(job2, "success", TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job2, "failure",
                TextOutputFormat.class,
                Text.class, Text.class);

        // broadcast client id and secret to workers
        jobConf2.set("clientID", clientID);
        jobConf2.set("clientSecret", clientSecret);

        // delete output path if it exist (LOCAL DEVELOPMENT ONLY)
        if (fileSystem.exists(new Path(outputSongs))) {
            fileSystem.delete(new Path(outputSongs), true);
        }
        // run job
        int job2ResultCode = job2.waitForCompletion(true) ? 0 : 1;

        long job2Time = System.nanoTime();

        // -------------------------------------------------------------------------------------------------------------
        // Job 3: Attach the playlists each song appears in to the end of its song data entry
        final Job job3 = Job.getInstance(conf, "SpotifyAppendPlaylistAppearancesToSongData");
        job3.setJarByClass(MRDataProcessor.class);
        final Configuration jobConf3 = job3.getConfiguration();
        jobConf3.set("mapreduce.output.textoutputformat.separator", "\t");

        job3.setMapperClass(PlaylistAppearancesMapper.class);
        job3.setCombinerClass(PlaylistAppearancesCombiner.class);
        job3.setReducerClass(PlaylistAppearancesReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(outputPlaylists));
        FileInputFormat.addInputPath(job3, new Path(outputSongs));
        FileOutputFormat.setOutputPath(job3, new Path(outputSongsWithLists));

        int job3ResultCode = job3.waitForCompletion(true) ? 0 : 1;
        if (job3ResultCode == 1) {
            return job3ResultCode;
        }


        // Report runtimes before exiting
        long end = System.nanoTime();
        System.out.println("\n --------------------------------- RUNTIME BREAKDOWN ---------------------------------");
        System.out.println("Total Time Elapsed....................: " + ((end-start) * Math.pow(10,-9)) + " seconds");
        System.out.println("Job0 - Get Playlists..................: " + ((job0Time-start) * Math.pow(10,-9)) + " seconds");
        System.out.println("Job1 - Parse Songs & Drop Duplicates..: " + ((job1Time-job0Time) * Math.pow(10,-9)) + " seconds");
        System.out.println("Job2 - Spotify API Calls..............: " + ((job2Time-job1Time) * Math.pow(10,-9)) + " seconds");
        System.out.println("Job3 - Append Playlist IDs............: " + ((end-job2Time) * Math.pow(10,-9)) + " seconds");
        System.out.println();

        return job3ResultCode;
    }


    public static void main(final String[] args) {
        // if user doesn't enter 5 arguments, throw error
        if (args.length != 7) {
            throw new Error("Input arguments required (7):\n<input-dir> <intermediate-dir> <output-playlists>" +
                    " <output-songs> <output-songs-with-lists> <client-id> <client-secret>");
        }

        try {
            ToolRunner.run(new MRDataProcessor(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
