package com.cineindex.companion.ui.player

import android.content.Context
import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.Surface
import androidx.compose.ui.Modifier
import androidx.media3.common.MediaItem
import com.cineindex.companion.ui.theme.CineIndexTheme
import dagger.hilt.android.AndroidEntryPoint
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

@AndroidEntryPoint
class PlayerActivity : ComponentActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()

        val mediaItemsJson = intent.getStringExtra(EXTRA_MEDIA_ITEMS) ?: "[]"
        val startIndex = intent.getIntExtra(EXTRA_START_INDEX, 0)
        val startPositionMs = intent.getLongExtra(EXTRA_START_POSITION, 0L)

        val listType = object : TypeToken<List<MediaItemData>>() {}.type
        val itemDataList: List<MediaItemData> = Gson().fromJson(mediaItemsJson, listType)

        val mediaItems = itemDataList.map { it.toMediaItem() }

        setContent {
            CineIndexTheme {
                Surface(modifier = Modifier.fillMaxSize()) {
                    PlayerScreen(
                        mediaItems = mediaItems,
                        startIndex = startIndex,
                        startPositionMs = startPositionMs,
                        onBack = { finish() }
                    )
                }
            }
        }
    }

    companion object {
        private const val EXTRA_MEDIA_ITEMS = "extra_media_items"
        private const val EXTRA_START_INDEX = "extra_start_index"
        private const val EXTRA_START_POSITION = "extra_start_position"

        /**
         * Launch the player activity with a playlist of media items.
         */
        fun launch(
            context: Context,
            mediaItems: List<MediaItem>,
            startIndex: Int = 0,
            startPositionMs: Long = 0L
        ) {
            val itemDataList = mediaItems.map { MediaItemData.fromMediaItem(it) }
            val mediaItemsJson = Gson().toJson(itemDataList)

            val intent = Intent(context, PlayerActivity::class.java).apply {
                putExtra(EXTRA_MEDIA_ITEMS, mediaItemsJson)
                putExtra(EXTRA_START_INDEX, startIndex)
                putExtra(EXTRA_START_POSITION, startPositionMs)
            }
            context.startActivity(intent)
        }
    }
}

/**
 * Serializable data class to pass MediaItem info through Intent.
 */
data class MediaItemData(
    val uri: String,
    val title: String,
    val subtitle: String
) {
    fun toMediaItem(): MediaItem {
        return MediaItem.Builder()
            .setUri(uri)
            .setMediaId(uri)
            .setMediaMetadata(
                androidx.media3.common.MediaMetadata.Builder()
                    .setTitle(title)
                    .setSubtitle(subtitle)
                    .build()
            )
            .build()
    }

    companion object {
        fun fromMediaItem(item: MediaItem): MediaItemData {
            return MediaItemData(
                uri = item.localConfiguration?.uri?.toString() ?: "",
                title = item.mediaMetadata.title?.toString() ?: "",
                subtitle = item.mediaMetadata.subtitle?.toString() ?: ""
            )
        }
    }
}
