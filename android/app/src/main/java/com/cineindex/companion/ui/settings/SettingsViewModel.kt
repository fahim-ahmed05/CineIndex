package com.cineindex.companion.ui.settings

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.cineindex.companion.data.config.AppPreferences
import com.cineindex.companion.data.db.MediaDatabaseProvider
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import javax.inject.Inject

@HiltViewModel
class SettingsViewModel @Inject constructor(
    private val appPreferences: AppPreferences,
    private val mediaDatabaseProvider: MediaDatabaseProvider
) : ViewModel() {

    val dbFolderUri: StateFlow<String?> = appPreferences.dbFolderUri
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), null)

    val downloadFolderUri: StateFlow<String?> = appPreferences.downloadFolderUri
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), null)

    private val _mediaCount = MutableStateFlow(0)
    val mediaCount: StateFlow<Int> = _mediaCount

    init {
        updateStats()
    }

    fun setDbFolderUri(uri: String) {
        viewModelScope.launch {
            appPreferences.setDbFolderUri(uri)
            // The SearchViewModel observes this and will auto-reload the DB
        }
    }

    fun setDownloadFolderUri(uri: String) {
        viewModelScope.launch {
            appPreferences.setDownloadFolderUri(uri)
        }
    }

    fun reloadDatabase() {
        // Clear and re-set the URI to force SearchViewModel to reload the database
        val uri = dbFolderUri.value
        if (uri != null) {
            viewModelScope.launch {
                appPreferences.setDbFolderUri("")
                kotlinx.coroutines.delay(100)
                appPreferences.setDbFolderUri(uri)
                updateStats()
            }
        } else {
            updateStats()
        }
    }

    private fun updateStats() {
        viewModelScope.launch {
            try {
                val db = mediaDatabaseProvider.getDatabase()
                if (db != null) {
                    _mediaCount.value = db.mediaDao().getCount()
                } else {
                    _mediaCount.value = 0
                }
            } catch (e: Exception) {
                _mediaCount.value = 0
            }
        }
    }
}
