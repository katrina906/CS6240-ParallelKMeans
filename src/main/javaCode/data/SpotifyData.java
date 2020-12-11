package javaCode.data;

import com.wrapper.spotify.SpotifyApi;
import com.wrapper.spotify.exceptions.SpotifyWebApiException;
import com.wrapper.spotify.model_objects.specification.Paging;
import com.wrapper.spotify.model_objects.specification.Track;
import com.wrapper.spotify.requests.authorization.client_credentials.ClientCredentialsRequest;
import com.wrapper.spotify.model_objects.credentials.ClientCredentials;
import com.wrapper.spotify.model_objects.specification.AudioFeatures;
import com.wrapper.spotify.requests.data.search.simplified.SearchTracksRequest;
import com.wrapper.spotify.requests.data.tracks.GetAudioFeaturesForSeveralTracksRequest;
import com.wrapper.spotify.requests.data.tracks.GetAudioFeaturesForTrackRequest;

import java.io.IOException;

import org.apache.hc.core5.http.ParseException;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

// Package and example code from: https://github.com/thelinmichael/spotify-web-api-java

public class SpotifyData {
    private static final Logger logger = LogManager.getLogger(SpotifyData.class);

    /**
     * Define spotifyAPI with clientID, secret, and access token
     */
    private final SpotifyApi spotifyApi;

    public SpotifyData(String clientID, String clientSecret) throws Exception {
        if (clientID == null || clientSecret == null) {
            throw new NullPointerException("clientID or clientSecret was null.");
        }
        this.spotifyApi = new SpotifyApi.Builder()
                .setClientId(clientID)
                .setClientSecret(clientSecret)
                .build();
        if (this.spotifyApi == null) {
            throw new IllegalStateException("Spotify API build failure.");
        }
        try {
            this.spotifyApi.setAccessToken(this.getCredentials());
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    /**
     * Get credentials using client secret and id input
     */
    private String getCredentials() throws ParseException, SpotifyWebApiException, IOException {
        final ClientCredentialsRequest clientCredentialsRequest = spotifyApi.clientCredentials()
                .build();
        final ClientCredentials clientCredentials = clientCredentialsRequest.execute();

        return clientCredentials.getAccessToken();
    }

    /**
     * Search for song features using one spotify track id
     */
    public AudioFeatures getAudioFeatures(String id) throws ParseException, SpotifyWebApiException, IOException {
        GetAudioFeaturesForTrackRequest getAudioFeaturesForTrackRequest = this.spotifyApi
                .getAudioFeaturesForTrack(id)
                .build();
        return getAudioFeaturesForTrackRequest.execute();
    }

    /**
     * Search for song features using list of spotify track ids (API takes max 100 songs at a time)
     */
    public AudioFeatures[] getAudioFeaturesList(String[] ids) throws ParseException, SpotifyWebApiException, IOException {
        GetAudioFeaturesForSeveralTracksRequest getAudioFeaturesForSeveralTracksRequest = this.spotifyApi
                .getAudioFeaturesForSeveralTracks(ids)
                .build();
        return getAudioFeaturesForSeveralTracksRequest.execute();
    }
}
