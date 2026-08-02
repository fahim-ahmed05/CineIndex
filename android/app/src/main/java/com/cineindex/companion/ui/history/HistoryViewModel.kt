package com.cineindex.companion.ui.history

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.cineindex.companion.data.db.HistoryDao
import com.cineindex.companion.data.db.HistoryEntity
import dagger.hilt.android.lifecycle.HiltViewModel
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import javax.inject.Inject

@HiltViewModel
class HistoryViewModel @Inject constructor(
    private val historyDao: HistoryDao
) : ViewModel() {

    val history: StateFlow<List<HistoryEntity>> = historyDao.getRecentHistory(20)
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5000), emptyList())

    fun addToHistory(url: String, filename: String, path: String, root: String) {
        viewModelScope.launch {
            historyDao.upsert(
                HistoryEntity(
                    url = url,
                    filename = filename,
                    path = path,
                    root = root,
                    playedAt = System.currentTimeMillis()
                )
            )
            historyDao.evictOldEntries(20)
        }
    }
}
