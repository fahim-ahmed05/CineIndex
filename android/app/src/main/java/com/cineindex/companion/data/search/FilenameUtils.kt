package com.cineindex.companion.data.search

/**
 * Filename display utilities ported from the Python TUI's main.py.
 * Handles dots-to-spaces conversion, episode detection, and sort key generation.
 */
object FilenameUtils {

    private val EPISODE_REGEX = Regex("""[sS](\d{1,2})[ ._-]*[eE](\d{1,3})""")

    // Patterns where dots should NOT be converted to spaces
    private val DOT_BLOCKLIST_PATTERNS = listOf(
        Regex("""\d+\.\d+"""),                  // "5.1" (audio), "2.0" (stereo)
        Regex("""\b(?:[A-Z]\.)+[A-Z]\b""")      // "S.H.I.E.L.D", "U.N.C.L.E"
    )

    // Metadata tags that should be separated by spaces when dots are replaced
    private val METADATA_TAG_RE = Regex(
        """\\.(?=(?:\d{3,4}p|BluRay|WEBRip|WEB-DL|BDRip|HDRip|x264|x265|HEVC|AAC|DTS|AC3|\dCH)\b)""",
        RegexOption.IGNORE_CASE
    )

    // Junk after the show name — brackets, parens, resolution tags etc.
    private val SHOW_NAME_STRIP_RE = Regex(
        """(\[.*?]|\(.*?\)|\d{3,4}p|BluRay|WEBRip|HDTV|x264|x265|HEVC|AAC|DTS|AC3|""" +
                """DUAL|MULTI|ESub|REPACK|PROPER|EXTENDED|UNRATED|THEATRICAL|DIRECTORS\.CUT)""",
        RegexOption.IGNORE_CASE
    )

    /**
     * Pretty-print a filename: optionally convert dots to spaces while preserving
     * the file extension and blocklisted patterns (audio formats, acronyms).
     */
    fun prettyFilename(filename: String, dotsToSpaces: Boolean = false): String {
        if (filename.isEmpty() || '.' !in filename) return filename
        if (!dotsToSpaces) return filename

        val lastDot = filename.lastIndexOf('.')
        val name = filename.substring(0, lastDot)
        val ext = filename.substring(lastDot + 1)

        // Find blocked positions
        val blockedPositions = mutableSetOf<Int>()
        for (pattern in DOT_BLOCKLIST_PATTERNS) {
            for (match in pattern.findAll(name)) {
                for (i in match.range) {
                    blockedPositions.add(i)
                }
            }
        }

        // Replace dots with spaces, except in blocked positions
        val result = StringBuilder()
        for (i in name.indices) {
            if (name[i] == '.' && i !in blockedPositions) {
                result.append(' ')
            } else {
                result.append(name[i])
            }
        }

        var display = result.toString()
        display = METADATA_TAG_RE.replace(display, " ")
        display = display.split(Regex("""\s+""")).filter { it.isNotEmpty() }.joinToString(" ")
        if (display.isEmpty()) display = name

        return "$display.$ext"
    }

    /**
     * Extract and normalize a show name from an episode filename.
     * Returns null if the filename doesn't contain an episode pattern.
     *
     * Examples:
     *   "Game.of.Thrones.S01E01.1080p.mkv" -> "gameofthrones"
     *   "Girls Hostel 2.0 S02E01.mp4"      -> "girlshostel20"
     *   "S01E01.mkv"                         -> null (no show prefix)
     */
    fun extractShowName(filename: String): String? {
        val match = EPISODE_REGEX.find(filename) ?: return null
        var prefix = filename.substring(0, match.range.first)
        prefix = SHOW_NAME_STRIP_RE.replace(prefix, "")
        val normalized = prefix.lowercase().replace(Regex("[^a-z0-9]"), "")
        return if (normalized.length < 3) null else normalized
    }

    /**
     * Check if a filename contains an episode pattern (SxxExx).
     */
    fun isEpisode(filename: String): Boolean = EPISODE_REGEX.containsMatchIn(filename)

    /**
     * Extract season and episode numbers from a filename.
     * Returns (season, episode) or null if not an episode.
     */
    fun parseEpisode(filename: String): Pair<Int, Int>? {
        val match = EPISODE_REGEX.find(filename) ?: return null
        val season = match.groupValues[1].toInt()
        val episode = match.groupValues[2].toInt()
        return Pair(season, episode)
    }

    /**
     * Sort key for episodes: (season, episode, filename).
     * Non-episodes sort to the end.
     */
    fun episodeSortKey(filename: String): Triple<Int, Int, String> {
        val parsed = parseEpisode(filename)
        return if (parsed != null) {
            Triple(parsed.first, parsed.second, filename.lowercase())
        } else {
            Triple(9999, 9999, filename.lowercase())
        }
    }

    /**
     * Format a size string into a human-readable format.
     */
    fun formatSize(sizeStr: String?): String? {
        if (sizeStr.isNullOrBlank()) return null
        val s = sizeStr.trim().uppercase()
        val match = Regex("""^([\d.]+)\s*([KMG]?B?)$""").find(s) ?: return sizeStr
        val numStr = match.groupValues[1]
        val unit = match.groupValues[2]
        val value = numStr.toDoubleOrNull() ?: return sizeStr

        val bytes = when (unit) {
            "K", "KB" -> value * 1024
            "M", "MB" -> value * 1024 * 1024
            "G", "GB" -> value * 1024 * 1024 * 1024
            else -> value
        }

        return when {
            bytes >= 1024 * 1024 * 1024 -> String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024))
            bytes >= 1024 * 1024 -> String.format("%.2f MB", bytes / (1024.0 * 1024))
            bytes >= 1024 -> String.format("%.2f KB", bytes / 1024.0)
            else -> String.format("%.0f B", bytes)
        }
    }
}
