package com.cineindex.companion.ui.player

import android.app.Activity
import android.content.ComponentName
import android.content.Context
import android.content.pm.ActivityInfo
import androidx.annotation.OptIn
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.view.WindowCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.WindowInsetsControllerCompat
import androidx.media3.common.MediaItem
import androidx.media3.common.util.UnstableApi
import androidx.media3.session.MediaController
import androidx.media3.session.SessionToken
import androidx.media3.ui.PlayerView
import com.cineindex.companion.player.PlaybackService
import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.MoreExecutors

@OptIn(UnstableApi::class)
@Composable
fun PlayerScreen(
    mediaItems: List<MediaItem>,
    startIndex: Int = 0,
    startPositionMs: Long = 0L,
    onBack: () -> Unit
) {
    val context = LocalContext.current
    var controllerFuture by remember { mutableStateOf<ListenableFuture<MediaController>?>(null) }
    var mediaController by remember { mutableStateOf<MediaController?>(null) }

    // Lock to landscape and hide system bars
    DisposableEffect(Unit) {
        val activity = context as? Activity
        if (activity != null) {
            val originalOrientation = activity.requestedOrientation
            activity.requestedOrientation = ActivityInfo.SCREEN_ORIENTATION_SENSOR_LANDSCAPE

            val window = activity.window
            val insetsController = WindowCompat.getInsetsController(window, window.decorView)
            insetsController.systemBarsBehavior = WindowInsetsControllerCompat.BEHAVIOR_SHOW_TRANSIENT_BARS_BY_SWIPE
            insetsController.hide(WindowInsetsCompat.Type.systemBars())

            onDispose {
                activity.requestedOrientation = originalOrientation
                insetsController.show(WindowInsetsCompat.Type.systemBars())
            }
        } else {
            onDispose { }
        }
    }

    // Connect to MediaController
    LaunchedEffect(Unit) {
        val sessionToken = SessionToken(context, ComponentName(context, PlaybackService::class.java))
        val future = MediaController.Builder(context, sessionToken).buildAsync()
        controllerFuture = future

        future.addListener(
            {
                val controller = future.get()
                mediaController = controller

                // Only set items if the controller isn't already playing this list
                // (handling configuration changes)
                if (controller.mediaItemCount == 0 || controller.currentMediaItem?.localConfiguration?.uri != mediaItems.getOrNull(startIndex)?.localConfiguration?.uri) {
                    controller.setMediaItems(mediaItems, startIndex, startPositionMs)
                    controller.prepare()
                    controller.play()
                }
            },
            MoreExecutors.directExecutor()
        )
    }

    // Release controller on dispose
    DisposableEffect(Unit) {
        onDispose {
            controllerFuture?.let { MediaController.releaseFuture(it) }
        }
    }

    // Render PlayerView
    AndroidView(
        factory = { ctx ->
            PlayerView(ctx).apply {
                useController = true
                setShowNextButton(true)
                setShowPreviousButton(true)
                // Add a back button to the controller UI
                setFullscreenButtonClickListener {
                    onBack()
                }
            }
        },
        update = { playerView ->
            playerView.player = mediaController
        },
        modifier = Modifier.fillMaxSize()
    )
}
