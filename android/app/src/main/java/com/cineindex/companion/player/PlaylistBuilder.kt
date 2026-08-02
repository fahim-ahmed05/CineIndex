package com.cineindex.companion.player

import android.net.Uri
import androidx.media3.common.MediaItem
import androidx.media3.common.MediaMetadata
import com.cineindex.companion.data.config.RootsConfig
import com.cineindex.companion.data.db.MediaDao
import com.cineindex.companion.data.db.MediaEntity
import com.cineindex.companion.data.search.FilenameUtils
import javax.inject.Inject

/**
 * Builds serial episode playlists, porting the logic from the Python TUI's build_dir_playlist().
 */
class PlaylistBuilder @Inject constructor(
    private val rootsConfig: RootsConfig
) {

    /**
     * Builds a playlist starting with the target media.
     * If the target is an episode, it attempts to find all other episodes of the same show
     * across the library using 3 fallback strategies.
     *
     * @return Pair containing the playlist of MediaItems and the startIndex of the target.
     */
    suspend fun buildPlaylist(target: MediaEntity, mediaDao: MediaDao): Pair<List<MediaItem>, Int> {
        val showName = FilenameUtils.extractShowName(target.filename)
        
        // If not an episode, just play the single file
        if (showName == null) {
            return Pair(listOf(target.toMediaItem()), 0)
        }

        val allCandidates = mutableListOf<MediaEntity>()

        // Strategy 1: Cross-root tag matching
        // Find all roots with the same tag as the target's root
        val sameTagRoots = rootsConfig.getRootsWithSameTag(target.root)
        if (sameTagRoots.size > 1) {
            val potentialMatches = mediaDao.getByRoots(sameTagRoots)
            allCandidates.addAll(potentialMatches.filter { 
                FilenameUtils.extractShowName(it.filename) == showName 
            })
        }

        // Strategy 2: Broad library search
        if (allCandidates.isEmpty()) {
            val allMedia = mediaDao.getAll()
            allCandidates.addAll(allMedia.filter { 
                FilenameUtils.extractShowName(it.filename) == showName 
            })
        }

        // Strategy 3: Same directory fallback
        if (allCandidates.isEmpty()) {
            allCandidates.addAll(mediaDao.getByPath(target.path))
        }

        // Always ensure the target itself is in the candidates
        if (allCandidates.none { it.url == target.url }) {
            allCandidates.add(target)
        }

        // Deduplicate and rank variants (prefer same folder, same root)
        val deduplicated = deduplicateAndRank(allCandidates, target)

        // Sort by episode number
        val sorted = deduplicated.sortedBy { FilenameUtils.episodeSortKey(it.filename).first * 1000 + FilenameUtils.episodeSortKey(it.filename).second }
        
        // Find start index
        var startIndex = sorted.indexOfFirst { it.url == target.url }
        if (startIndex == -1) startIndex = 0

        val mediaItems = sorted.map { it.toMediaItem() }
        return Pair(mediaItems, startIndex)
    }

    /**
     * Deduplicates episodes that appear in multiple roots/qualities.
     * Ranks variants preferring those closest to the target (same path > same root > others).
     */
    private fun deduplicateAndRank(candidates: List<MediaEntity>, target: MediaEntity): List<MediaEntity> {
        val grouped = mutableMapOf<Pair<Int, Int>, MutableList<MediaEntity>>()
        
        for (candidate in candidates) {
            val epInfo = FilenameUtils.parseEpisode(candidate.filename) ?: continue
            grouped.getOrPut(epInfo) { mutableListOf() }.add(candidate)
        }

        val result = mutableListOf<MediaEntity>()
        
        for ((_, variants) in grouped) {
            if (variants.size == 1) {
                result.add(variants.first())
                continue
            }

            // Rank variants
            val bestVariant = variants.minByOrNull { variant ->
                when {
                    variant.url == target.url -> 0 // Target is always best for its own slot
                    variant.path == target.path -> 1 // Same folder
                    variant.path.startsWith(target.path.substringBeforeLast('/')) -> 2 // Same parent folder (e.g., Season 1 vs Season 2 folders)
                    variant.root == target.root -> 3 // Same root
                    else -> 4 // Other roots
                }
            }
            
            if (bestVariant != null) {
                result.add(bestVariant)
            }
        }

        return result
    }

    private fun MediaEntity.toMediaItem(): MediaItem {
        val displayTitle = FilenameUtils.prettyFilename(this.filename, rootsConfig.dotsToSpaces(this.root))
        return MediaItem.Builder()
            .setUri(Uri.parse(this.url))
            .setMediaId(this.url)
            .setMediaMetadata(
                MediaMetadata.Builder()
                    .setTitle(displayTitle)
                    .setSubtitle(this.path)
                    .build()
            )
            .build()
    }
}
