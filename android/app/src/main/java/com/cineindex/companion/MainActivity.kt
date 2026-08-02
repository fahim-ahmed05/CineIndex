package com.cineindex.companion

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.Surface
import androidx.compose.ui.Modifier
import com.cineindex.companion.ui.CineIndexNavHost
import com.cineindex.companion.ui.theme.CineIndexTheme
import dagger.hilt.android.AndroidEntryPoint

@AndroidEntryPoint
class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()
        setContent {
            CineIndexTheme {
                Surface(modifier = Modifier.fillMaxSize()) {
                    CineIndexNavHost()
                }
            }
        }
    }
}
