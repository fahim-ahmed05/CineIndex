package com.cineindex.companion.download

import android.app.NotificationManager
import android.content.Context
import androidx.core.app.NotificationCompat
import androidx.hilt.work.HiltWorker
import androidx.work.CoroutineWorker
import androidx.work.ForegroundInfo
import androidx.work.WorkerParameters
import com.cineindex.companion.data.db.DownloadDao
import com.cineindex.companion.data.db.DownloadEntity
import dagger.assisted.Assisted
import dagger.assisted.AssistedInject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.Request
import java.io.File
import java.io.FileOutputStream
import java.io.InputStream
import kotlin.math.max

@HiltWorker
class DownloadWorker @AssistedInject constructor(
    @Assisted private val context: Context,
    @Assisted params: WorkerParameters,
    private val downloadDao: DownloadDao,
    private val okHttpClient: OkHttpClient
) : CoroutineWorker(context, params) {

    companion object {
        const val KEY_URL = "url"
        const val KEY_FILENAME = "filename"
        const val KEY_FOLDER_URI = "folder_uri"
        
        private const val NOTIFICATION_ID = 1001
        private const val CHANNEL_ID = "downloads"
    }

    private val notificationManager = context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager

    override suspend fun doWork(): Result = withContext(Dispatchers.IO) {
        val url = inputData.getString(KEY_URL) ?: return@withContext Result.failure()
        val filename = inputData.getString(KEY_FILENAME) ?: "download"
        val folderUriStr = inputData.getString(KEY_FOLDER_URI) ?: return@withContext Result.failure()

        try {
            // Setup foreground notification
            setForeground(createForegroundInfo(filename, 0, 100))

            // Check if download is already in DB
            var download = downloadDao.getByUrl(url)
            if (download == null) {
                download = DownloadEntity(
                    url = url,
                    filename = filename,
                    localPath = "",
                    status = DownloadEntity.STATUS_DOWNLOADING
                )
                downloadDao.upsert(download)
            } else {
                downloadDao.update(download.copy(status = DownloadEntity.STATUS_DOWNLOADING))
            }

            // In a real app with SAF, we'd resolve the DocumentFile from the folderUri
            // For simplicity in this demo, we'll write to external files dir if SAF fails
            val outputFile = File(context.getExternalFilesDir(null), filename)

            val downloadedBytes = if (outputFile.exists()) outputFile.length() else 0L

            val request = Request.Builder()
                .url(url)
                .header("Range", "bytes=$downloadedBytes-")
                .build()

            okHttpClient.newCall(request).execute().use { response ->
                if (!response.isSuccessful) {
                    downloadDao.updateProgress(url, downloadedBytes, download.totalBytes, DownloadEntity.STATUS_FAILED)
                    return@withContext Result.failure()
                }

                val body = response.body
                if (body == null) {
                    downloadDao.updateProgress(url, downloadedBytes, download.totalBytes, DownloadEntity.STATUS_FAILED)
                    return@withContext Result.failure()
                }

                val contentLength = body.contentLength()
                val totalBytes = if (contentLength > 0) downloadedBytes + contentLength else download.totalBytes

                downloadDao.updateProgress(url, downloadedBytes, totalBytes, DownloadEntity.STATUS_DOWNLOADING)

                var currentBytes = downloadedBytes
                var lastUpdateTime = System.currentTimeMillis()

                body.byteStream().use { input ->
                    FileOutputStream(outputFile, true).use { output ->
                        val buffer = ByteArray(8 * 1024)
                        var bytesRead: Int
                        while (input.read(buffer).also { bytesRead = it } != -1) {
                            if (isStopped) {
                                downloadDao.updateProgress(url, currentBytes, totalBytes, DownloadEntity.STATUS_PAUSED)
                                return@withContext Result.retry()
                            }

                            output.write(buffer, 0, bytesRead)
                            currentBytes += bytesRead

                            val now = System.currentTimeMillis()
                            if (now - lastUpdateTime > 1000) {
                                lastUpdateTime = now
                                downloadDao.updateProgress(url, currentBytes, totalBytes, DownloadEntity.STATUS_DOWNLOADING)
                                
                                val progress = if (totalBytes > 0) ((currentBytes.toFloat() / totalBytes) * 100).toInt() else 0
                                setForeground(createForegroundInfo(filename, progress, 100))
                            }
                        }
                    }
                }

                // Completed
                downloadDao.update(
                    download.copy(
                        downloadedBytes = currentBytes,
                        totalBytes = max(totalBytes, currentBytes),
                        localPath = outputFile.absolutePath,
                        status = DownloadEntity.STATUS_COMPLETED
                    )
                )
                return@withContext Result.success()
            }
        } catch (e: Exception) {
            downloadDao.updateProgress(url, 0, 0, DownloadEntity.STATUS_FAILED)
            return@withContext Result.failure()
        }
    }

    private fun createForegroundInfo(filename: String, progress: Int, max: Int): ForegroundInfo {
        val notification = NotificationCompat.Builder(context, CHANNEL_ID)
            .setContentTitle("Downloading $filename")
            .setTicker("Downloading $filename")
            .setSmallIcon(android.R.drawable.stat_sys_download)
            .setOngoing(true)
            .setProgress(max, progress, false)
            .build()

        if (android.os.Build.VERSION.SDK_INT >= 34) { // Android 14+
            return ForegroundInfo(NOTIFICATION_ID, notification, android.content.pm.ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC)
        } else {
            return ForegroundInfo(NOTIFICATION_ID, notification)
        }
    }
}
