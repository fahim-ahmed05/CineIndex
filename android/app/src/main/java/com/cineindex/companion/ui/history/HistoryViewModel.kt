package com.cineindex.companion.ui.history

import android.content.Context
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.cineindex.companion.data.db.HistoryDao
import com.cineindex.companion.data.db.HistoryEntity
import com.cineindex.companion.ui.player.PlayerActivity
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import java.util.concurrent.TimeUnit
import javax.inject.Inject

@HiltViewModel
class HistoryViewModel @Inject constructor(
    private val historyDao: HistoryDao
) : ViewModel() {

    val historyItems: StateFlow<List<HistoryEntity>> = historyDao.getRecentHistory(50)
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    fun deleteHistory(url: String) {
        viewModelScope.launch {
            historyDao.deleteByUrl(url)
        }
    }

    fun playFromHistory(history: HistoryEntity, context: Context) {
        val mediaItem = androidx.media3.common.MediaItem.Builder()
            .setUri(history.url)
            .setMediaId(history.url)
            .setMediaMetadata(
                androidx.media3.common.MediaMetadata.Builder()
                    .setTitle(history.filename)
                    .setSubtitle(history.path)
                    .build()
            )
            .build()
            
        PlayerActivity.launch(context, listOf(mediaItem), 0, history.positionMs)
    }

    fun getRelativeTime(epochMs: Long): String {
        val now = System.currentTimeMillis()
        val diff = now - epochMs

        return when {
            diff < TimeUnit.MINUTES.toMillis(1) -> "Just now"
            diff < TimeUnit.HOURS.toMillis(1) -> {
                val mins = TimeUnit.MILLISECONDS.toMinutes(diff)
                "${mins}m ago"
            }
            diff < TimeUnit.DAYS.toMillis(1) -> {
                val hours = TimeUnit.MILLISECONDS.toHours(diff)
                "${hours}h ago"
            }
            diff < TimeUnit.DAYS.toMillis(7) -> {
                val days = TimeUnit.MILLISECONDS.toDays(diff)
                "${days}d ago"
            }
            diff < TimeUnit.DAYS.toMillis(30) -> {
                val weeks = TimeUnit.MILLISECONDS.toDays(diff) / 7
                "${weeks}w ago"
            }
            else -> {
                val months = TimeUnit.MILLISECONDS.toDays(diff) / 30
                "${months}mo ago"
            }
        }
    }
}
