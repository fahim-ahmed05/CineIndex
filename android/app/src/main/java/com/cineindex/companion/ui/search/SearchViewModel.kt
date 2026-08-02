package com.cineindex.companion.ui.search

import android.content.Context
import android.net.Uri
import androidx.documentfile.provider.DocumentFile
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.cineindex.companion.data.config.AppPreferences
import com.cineindex.companion.data.config.RootsConfig
import com.cineindex.companion.data.db.DownloadDao
import com.cineindex.companion.data.db.DownloadEntity
import com.cineindex.companion.data.db.MediaDatabaseProvider
import com.cineindex.companion.data.db.MediaDao
import com.cineindex.companion.data.db.MediaEntity
import com.cineindex.companion.data.search.FilenameUtils
import com.cineindex.companion.data.search.SearchEngine
import com.cineindex.companion.player.PlaylistBuilder
import com.cineindex.companion.download.DownloadRepository
import com.cineindex.companion.ui.player.PlayerActivity
import dagger.hilt.android.lifecycle.HiltViewModel
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import java.io.File
import javax.inject.Inject

@OptIn(FlowPreview::class)
@HiltViewModel
class SearchViewModel @Inject constructor(
    @ApplicationContext private val appContext: Context,
    private val mediaDatabaseProvider: MediaDatabaseProvider,
    private val rootsConfig: RootsConfig,
    private val appPreferences: AppPreferences,
    private val downloadDao: DownloadDao,
    private val playlistBuilder: PlaylistBuilder,
    private val downloadRepository: DownloadRepository,
) : ViewModel() {

    private val _searchQuery = MutableStateFlow("")
    val searchQuery: StateFlow<String> = _searchQuery

    private val _searchResults = MutableStateFlow<List<MediaEntity>>(emptyList())
    val searchResults: StateFlow<List<MediaEntity>> = _searchResults

    private val _isLoading = MutableStateFlow(false)
    val isLoading: StateFlow<Boolean> = _isLoading

    private val _mediaCount = MutableStateFlow(0)
    val mediaCount: StateFlow<Int> = _mediaCount

    private val _selectedItems = MutableStateFlow<Set<String>>(emptySet())
    val selectedItems: StateFlow<Set<String>> = _selectedItems

    private val _dbLoaded = MutableStateFlow(false)
    val dbLoaded: StateFlow<Boolean> = _dbLoaded

    val isMultiSelectMode: StateFlow<Boolean> = _selectedItems.map { it.isNotEmpty() }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(), false)

    private var searchEngine: SearchEngine? = null
    private var mediaDao: MediaDao? = null

    init {
        // Load DB when folder URI is available
        viewModelScope.launch {
            appPreferences.dbFolderUri.collect { uriStr ->
                if (uriStr != null) {
                    loadDatabase(uriStr)
                }
            }
        }

        // Debounced search
        viewModelScope.launch {
            _searchQuery
                .debounce(300)
                .distinctUntilChanged()
                .collect { query ->
                    performSearch(query)
                }
        }
    }

    private suspend fun loadDatabase(folderUriStr: String) {
        try {
            val folderUri = Uri.parse(folderUriStr)

            // Try to find media_index.db in the folder
            val docFile = DocumentFile.fromTreeUri(appContext, folderUri)
            val dbDocFile = docFile?.findFile("media_index.db")
            val rootsDocFile = docFile?.findFile("roots.json")

            if (dbDocFile == null || !dbDocFile.exists()) {
                _dbLoaded.value = false
                return
            }

            // Copy DB to internal storage for Room to open (Room can't open SAF URIs directly)
            val internalDbFile = File(appContext.filesDir, "media_index.db")
            appContext.contentResolver.openInputStream(dbDocFile.uri)?.use { input ->
                internalDbFile.outputStream().use { output ->
                    input.copyTo(output)
                }
            }

            // Load roots.json if available
            if (rootsDocFile != null && rootsDocFile.exists()) {
                val internalRootsFile = File(appContext.filesDir, "roots.json")
                appContext.contentResolver.openInputStream(rootsDocFile.uri)?.use { input ->
                    internalRootsFile.outputStream().use { output ->
                        input.copyTo(output)
                    }
                }
                rootsConfig.load(internalRootsFile)
            }

            // Open database
            val db = mediaDatabaseProvider.open(appContext, internalDbFile)
            if (db != null) {
                val dao = db.mediaDao()
                mediaDao = dao
                searchEngine = SearchEngine(dao)
                _mediaCount.value = dao.getCount()
                _dbLoaded.value = true
            }
        } catch (e: Exception) {
            _dbLoaded.value = false
        }
    }

    fun onSearchQueryChanged(query: String) {
        _searchQuery.value = query
    }

    private suspend fun performSearch(query: String) {
        val engine = searchEngine ?: return
        if (query.isBlank()) {
            _searchResults.value = emptyList()
            return
        }

        _isLoading.value = true
        try {
            _searchResults.value = engine.search(query)
        } catch (e: Exception) {
            _searchResults.value = emptyList()
        } finally {
            _isLoading.value = false
        }
    }

    fun getDisplayFilename(media: MediaEntity): String {
        val dotsToSpaces = rootsConfig.dotsToSpaces(media.root)
        return FilenameUtils.prettyFilename(media.filename, dotsToSpaces)
    }

    fun getFormattedSize(media: MediaEntity): String? {
        return FilenameUtils.formatSize(media.size)
    }

    // --- Selection ---

    fun toggleSelection(url: String) {
        _selectedItems.value = _selectedItems.value.toMutableSet().apply {
            if (contains(url)) remove(url) else add(url)
        }
    }

    fun clearSelection() {
        _selectedItems.value = emptySet()
    }

    // --- Actions ---

    fun playMedia(media: MediaEntity, context: Context) {
        viewModelScope.launch {
            try {
                val dao = mediaDao ?: return@launch
                val (playlist, startIndex) = playlistBuilder.buildPlaylist(media, dao)
                PlayerActivity.launch(context, playlist, startIndex, 0L)
            } catch (e: Exception) {
                // Ignore for now
            }
        }
    }

    fun playSelected(context: Context) {
        viewModelScope.launch {
            val results = _searchResults.value
            val playlist = _selectedItems.value.mapNotNull { url ->
                val media = results.find { it.url == url }
                media?.let {
                    androidx.media3.common.MediaItem.Builder()
                        .setUri(it.url)
                        .setMediaId(it.url)
                        .setMediaMetadata(
                            androidx.media3.common.MediaMetadata.Builder()
                                .setTitle(FilenameUtils.prettyFilename(it.filename, rootsConfig.dotsToSpaces(it.root)))
                                .setSubtitle(it.path)
                                .build()
                        )
                        .build()
                }
            }

            if (playlist.isNotEmpty()) {
                PlayerActivity.launch(context, playlist, 0, 0L)
            }
            clearSelection()
        }
    }

    fun downloadMedia(media: MediaEntity) {
        viewModelScope.launch {
            downloadRepository.enqueueDownload(media.url, media.filename)
        }
    }

    fun downloadSelected() {
        viewModelScope.launch {
            val results = _searchResults.value
            for (url in _selectedItems.value) {
                val media = results.find { it.url == url } ?: continue
                downloadRepository.enqueueDownload(media.url, media.filename)
            }
            clearSelection()
        }
    }
}
