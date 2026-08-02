package com.cineindex.companion.ui.search

import android.content.Context
import android.net.Uri
import androidx.documentfile.provider.DocumentFile
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.cineindex.companion.data.config.AppPreferences
import com.cineindex.companion.data.config.RootsConfig
import com.cineindex.companion.data.db.MediaDatabaseProvider
import com.cineindex.companion.data.db.MediaDao
import com.cineindex.companion.data.db.MediaEntity
import com.cineindex.companion.data.search.FilenameUtils
import com.cineindex.companion.data.search.SearchEngine
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

    private val _dbLoaded = MutableStateFlow(true)
    val dbLoaded: StateFlow<Boolean> = _dbLoaded

    val isMultiSelectMode: StateFlow<Boolean> = _selectedItems.map { it.isNotEmpty() }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(), false)

    private var searchEngine: SearchEngine? = null
    private var mediaDao: MediaDao? = null

    init {
        // Initial fast-load from internal storage
        viewModelScope.launch {
            loadInternalDatabase()
        }

        // Sync from SAF only when the URI actually changes (e.g., set for the first time or manually reloaded)
        viewModelScope.launch {
            appPreferences.dbFolderUri.drop(1).collect { uriStr ->
                if (!uriStr.isNullOrEmpty()) {
                    syncDatabaseFromSaf(uriStr)
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

    private suspend fun loadInternalDatabase() {
        kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) {
            val dbFile = appContext.getDatabasePath("media_readonly")
            val internalRootsFile = File(appContext.filesDir, "roots.json")

            if (dbFile.exists()) {
                try {
                    if (internalRootsFile.exists()) rootsConfig.load(internalRootsFile)
                    val dao = mediaDatabaseProvider.open(appContext, dbFile)
                    if (dao != null) {
                        mediaDao = dao
                        searchEngine = SearchEngine(dao)
                        _mediaCount.value = dao.getCount()
                        _dbLoaded.value = true
                        return@withContext
                    }
                } catch (e: Exception) {
                    android.util.Log.e("CineIndex", "Database load failed", e)
                    // If Room fails to open (e.g. schema mismatch, corruption, missing hash), 
                    // delete the invalid file to prevent crash loops and reset state.
                    mediaDatabaseProvider.close()
                    dbFile.delete()
                    appContext.getDatabasePath("media_readonly-shm").delete()
                    appContext.getDatabasePath("media_readonly-wal").delete()
                    _dbLoaded.value = false
                    return@withContext
                }
            }
            _dbLoaded.value = false
        }
    }

    private suspend fun syncDatabaseFromSaf(folderUriStr: String) {
        kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) {
            mediaDatabaseProvider.setLoading(true)
            try {
                val folderUri = Uri.parse(folderUriStr)
                val docFile = DocumentFile.fromTreeUri(appContext, folderUri)
                
                var dbDocFile = docFile?.findFile("media_index.db")
                if (dbDocFile == null) {
                    dbDocFile = docFile?.listFiles()?.find {
                        val name = it.name ?: ""
                        name == "media_index" || (name.startsWith("media_index") && name.endsWith(".db"))
                    }
                }

                var rootsDocFile = docFile?.findFile("roots.json")
                if (rootsDocFile == null) {
                    rootsDocFile = docFile?.listFiles()?.find {
                        val name = it.name ?: ""
                        name == "roots" || (name.startsWith("roots") && name.endsWith(".json"))
                    }
                }

                if (dbDocFile == null || !dbDocFile.exists()) {
                    // Fallback to internal database if SAF is disconnected/missing
                    loadInternalDatabase()
                    return@withContext
                }

                val dbFile = appContext.getDatabasePath("media_readonly")
                dbFile.parentFile?.mkdirs()
                
                val internalRootsFile = File(appContext.filesDir, "roots.json")

                // Close any existing open database before overwriting the file to prevent corruption!
                mediaDatabaseProvider.close()
                appContext.getDatabasePath("media_readonly-shm").delete()
                appContext.getDatabasePath("media_readonly-wal").delete()

                // Copy DB to internal storage
                appContext.contentResolver.openInputStream(dbDocFile.uri)?.use { input ->
                    dbFile.outputStream().use { output ->
                        input.copyTo(output)
                    }
                }

                if (rootsDocFile != null && rootsDocFile.exists()) {
                    appContext.contentResolver.openInputStream(rootsDocFile.uri)?.use { input ->
                        internalRootsFile.outputStream().use { output ->
                            input.copyTo(output)
                        }
                    }
                }

                // After syncing, load it
                loadInternalDatabase()
            } catch (e: Exception) {
                _dbLoaded.value = false
            } finally {
                mediaDatabaseProvider.setLoading(false)
            }
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

    // --- Actions ---

    fun playMedia(media: MediaEntity, context: Context) {
        viewModelScope.launch {
            try {
                // Instantly record to history
                // We'll inject HistoryViewModel in the UI and call it from there, or we can just send an intent here.
                // Wait, it's cleaner to let the UI handle the Intent because it has the Activity context.
                // So we don't even need playMedia in the ViewModel anymore.
            } catch (e: Exception) {
                // Ignore for now
            }
        }
    }
}
