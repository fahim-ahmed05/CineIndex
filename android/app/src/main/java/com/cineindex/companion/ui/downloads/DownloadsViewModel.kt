package com.cineindex.companion.ui.downloads

import android.content.Context
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.cineindex.companion.data.db.DownloadDao
import com.cineindex.companion.data.db.DownloadEntity
import com.cineindex.companion.download.DownloadRepository
import com.cineindex.companion.ui.player.PlayerActivity
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import javax.inject.Inject
import java.io.File
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.combine

@HiltViewModel
class DownloadsViewModel @Inject constructor(
    private val downloadDao: DownloadDao,
    private val downloadRepository: DownloadRepository
) : ViewModel() {

    private val _searchQuery = MutableStateFlow("")
    val searchQuery: StateFlow<String> = _searchQuery

    val activeDownloads: StateFlow<List<DownloadEntity>> = combine(
        downloadDao.observeActive(),
        _searchQuery
    ) { list, query ->
        if (query.isBlank()) list else list.filter { it.filename.contains(query, ignoreCase = true) }
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val completedDownloads: StateFlow<List<DownloadEntity>> = combine(
        downloadDao.observeCompleted(),
        _searchQuery
    ) { list, query ->
        if (query.isBlank()) list else list.filter { it.filename.contains(query, ignoreCase = true) }
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    val hasAnyDownloads: StateFlow<Boolean> = combine(
        downloadDao.observeActive(),
        downloadDao.observeCompleted()
    ) { active, completed ->
        active.isNotEmpty() || completed.isNotEmpty()
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), false)

    fun onSearchQueryChanged(query: String) {
        _searchQuery.value = query
    }

    fun cancelDownload(url: String) {
        downloadRepository.cancelDownload(url)
        viewModelScope.launch {
            val download = downloadDao.getByUrl(url)
            if (download != null) {
                downloadDao.update(download.copy(status = DownloadEntity.STATUS_FAILED))
            }
        }
    }

    fun deleteDownload(download: DownloadEntity) {
        viewModelScope.launch {
            downloadRepository.deleteDownload(download)
        }
    }

    fun playLocal(download: DownloadEntity, context: Context) {
        if (download.localPath.isEmpty() || !File(download.localPath).exists()) return
        
        val uri = "file://${download.localPath}"
        val mediaItem = androidx.media3.common.MediaItem.Builder()
            .setUri(uri)
            .setMediaId(uri)
            .setMediaMetadata(
                androidx.media3.common.MediaMetadata.Builder()
                    .setTitle(download.filename)
                    .build()
            )
            .build()
            
        PlayerActivity.launch(context, listOf(mediaItem), 0, 0L)
    }
}
