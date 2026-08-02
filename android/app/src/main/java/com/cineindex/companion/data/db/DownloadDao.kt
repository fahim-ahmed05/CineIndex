package com.cineindex.companion.data.db

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import androidx.room.Update
import kotlinx.coroutines.flow.Flow

@Dao
interface DownloadDao {

    /**
     * Observe all downloads (active first, then completed, ordered by start time).
     */
    @Query("""
        SELECT * FROM downloads 
        ORDER BY 
            CASE status 
                WHEN 1 THEN 0  -- DOWNLOADING first
                WHEN 0 THEN 1  -- PENDING next
                WHEN 2 THEN 2  -- PAUSED
                WHEN 3 THEN 3  -- COMPLETED
                WHEN 4 THEN 4  -- FAILED
            END,
            startedAt DESC
    """)
    fun observeAll(): Flow<List<DownloadEntity>>

    /**
     * Get active downloads (pending or downloading).
     */
    @Query("SELECT * FROM downloads WHERE status IN (0, 1) ORDER BY startedAt DESC")
    fun observeActive(): Flow<List<DownloadEntity>>

    /**
     * Get completed downloads.
     */
    @Query("SELECT * FROM downloads WHERE status = 3 ORDER BY startedAt DESC")
    fun observeCompleted(): Flow<List<DownloadEntity>>

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(download: DownloadEntity)

    @Update
    suspend fun update(download: DownloadEntity)

    @Query("SELECT * FROM downloads WHERE url = :url")
    suspend fun getByUrl(url: String): DownloadEntity?

    @Query("DELETE FROM downloads WHERE url = :url")
    suspend fun deleteByUrl(url: String)

    @Query("DELETE FROM downloads WHERE url IN (:urls)")
    suspend fun deleteByUrls(urls: List<String>)

    /**
     * Update download progress.
     */
    @Query("UPDATE downloads SET downloadedBytes = :downloadedBytes, totalBytes = :totalBytes, status = :status WHERE url = :url")
    suspend fun updateProgress(url: String, downloadedBytes: Long, totalBytes: Long, status: Int)
}
