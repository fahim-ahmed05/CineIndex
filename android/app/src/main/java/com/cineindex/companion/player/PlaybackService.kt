package com.cineindex.companion.player

import android.content.Intent
import androidx.media3.common.MediaItem
import androidx.media3.common.Player
import androidx.media3.exoplayer.ExoPlayer
import androidx.media3.session.MediaSession
import androidx.media3.session.MediaSessionService
import com.cineindex.companion.data.db.HistoryDao
import com.cineindex.companion.data.db.HistoryEntity
import dagger.hilt.android.AndroidEntryPoint
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import javax.inject.Inject

@AndroidEntryPoint
class PlaybackService : MediaSessionService() {

    private var mediaSession: MediaSession? = null
    private var player: ExoPlayer? = null
    private val serviceScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    @Inject
    lateinit var historyDao: HistoryDao

    override fun onCreate() {
        super.onCreate()
        
        player = ExoPlayer.Builder(this).build().apply {
            addListener(object : Player.Listener {
                override fun onMediaItemTransition(mediaItem: MediaItem?, reason: Int) {
                    super.onMediaItemTransition(mediaItem, reason)
                    recordHistory(mediaItem, 0L)
                }

                override fun onIsPlayingChanged(isPlaying: Boolean) {
                    super.onIsPlayingChanged(isPlaying)
                    if (!isPlaying) {
                        // Paused - save current position
                        val currentItem = currentMediaItem
                        val position = currentPosition
                        if (currentItem != null && position > 0) {
                            recordHistory(currentItem, position)
                        }
                    }
                }

                override fun onPlaybackStateChanged(playbackState: Int) {
                    super.onPlaybackStateChanged(playbackState)
                    if (playbackState == Player.STATE_ENDED) {
                        // Finished playing - could mark as fully watched or advance
                        val currentItem = currentMediaItem
                        if (currentItem != null) {
                            recordHistory(currentItem, currentPosition, isCompleted = true)
                        }
                    }
                }
            })
        }

        mediaSession = MediaSession.Builder(this, player!!)
            .build()
    }

    private fun recordHistory(mediaItem: MediaItem?, positionMs: Long, isCompleted: Boolean = false) {
        if (mediaItem == null) return
        val url = mediaItem.localConfiguration?.uri?.toString() ?: return
        val title = mediaItem.mediaMetadata.title?.toString() ?: "Unknown"
        val currentDuration = player?.duration ?: 0L

        serviceScope.launch {
            // Check if we already have it to preserve start time, or create new
            val existing = historyDao.getByUrl(url)
            val duration = currentDuration.takeIf { it > 0 } ?: existing?.durationMs ?: 0L
            
            // If completed, keep position at end, otherwise use provided position
            val savedPosition = if (isCompleted) duration else positionMs
            
            val entry = existing?.copy(
                playedAt = System.currentTimeMillis(),
                positionMs = savedPosition,
                durationMs = duration
            ) ?: HistoryEntity(
                url = url,
                filename = title,
                path = mediaItem.mediaMetadata.subtitle?.toString() ?: "", // We pass path in subtitle
                root = "", // We can extract or pass via extras if needed
                playedAt = System.currentTimeMillis(),
                positionMs = savedPosition,
                durationMs = duration
            )

            historyDao.upsert(entry)
            historyDao.evictOldEntries(50) // Keep history bounded
        }
    }

    override fun onGetSession(controllerInfo: MediaSession.ControllerInfo): MediaSession? {
        return mediaSession
    }

    override fun onDestroy() {
        mediaSession?.run {
            player.release()
            release()
            mediaSession = null
        }
        super.onDestroy()
    }
}
