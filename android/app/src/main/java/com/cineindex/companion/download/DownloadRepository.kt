package com.cineindex.companion.download

import android.content.Context
import androidx.work.Data
import androidx.work.ExistingWorkPolicy
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkManager
import com.cineindex.companion.data.config.AppPreferences
import com.cineindex.companion.data.db.DownloadDao
import com.cineindex.companion.data.db.DownloadEntity
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.flow.first
import java.io.File
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class DownloadRepository @Inject constructor(
    @ApplicationContext private val context: Context,
    private val downloadDao: DownloadDao,
    private val appPreferences: AppPreferences
) {
    private val workManager = WorkManager.getInstance(context)

    suspend fun enqueueDownload(url: String, filename: String) {
        val folderUri = appPreferences.downloadFolderUri.first() ?: ""
        
        val inputData = Data.Builder()
            .putString(DownloadWorker.KEY_URL, url)
            .putString(DownloadWorker.KEY_FILENAME, filename)
            .putString(DownloadWorker.KEY_FOLDER_URI, folderUri)
            .build()

        val request = OneTimeWorkRequestBuilder<DownloadWorker>()
            .setInputData(inputData)
            .build()

        // Use url as unique work name so we don't enqueue duplicates
        workManager.enqueueUniqueWork(
            url,
            ExistingWorkPolicy.KEEP,
            request
        )
    }

    fun cancelDownload(url: String) {
        workManager.cancelUniqueWork(url)
    }

    suspend fun deleteDownload(download: DownloadEntity) {
        // Cancel if running
        cancelDownload(download.url)
        
        // Delete actual file if it exists
        if (download.localPath.isNotEmpty()) {
            val file = File(download.localPath)
            if (file.exists()) {
                file.delete()
            }
        }
        
        // Remove from DB
        downloadDao.deleteByUrl(download.url)
    }
}
