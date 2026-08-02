package com.cineindex.companion.data.db

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import kotlinx.coroutines.flow.Flow

@Dao
interface HistoryDao {

    /**
     * Get recent history, most recent first, limited to [limit] entries.
     */
    @Query("SELECT * FROM history ORDER BY playedAt DESC LIMIT :limit")
    fun getRecentHistory(limit: Int = 20): Flow<List<HistoryEntity>>

    /**
     * Insert or update a history entry (upsert by URL).
     */
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(entry: HistoryEntity)

    /**
     * Keep only the most recent N entries, evicting the oldest.
     */
    @Query("""
        DELETE FROM history WHERE url NOT IN (
            SELECT url FROM history ORDER BY playedAt DESC LIMIT :keep
        )
    """)
    suspend fun evictOldEntries(keep: Int = 20)

    /**
     * Get total history count.
     */
    @Query("SELECT COUNT(*) FROM history")
    suspend fun getCount(): Int
}
