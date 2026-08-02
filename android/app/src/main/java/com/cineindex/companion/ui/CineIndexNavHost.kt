package com.cineindex.companion.ui

import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Download
import androidx.compose.material.icons.filled.History
import androidx.compose.material.icons.filled.Search
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material.icons.outlined.Download
import androidx.compose.material.icons.outlined.History
import androidx.compose.material.icons.outlined.Search
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.material3.MaterialTheme
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.navigation.NavDestination.Companion.hierarchy
import androidx.navigation.NavGraph.Companion.findStartDestination
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.currentBackStackEntryAsState
import androidx.navigation.compose.rememberNavController
import com.cineindex.companion.ui.downloads.DownloadsScreen
import com.cineindex.companion.ui.history.HistoryScreen
import com.cineindex.companion.ui.search.SearchScreen
import com.cineindex.companion.ui.settings.SettingsScreen

sealed class Screen(
    val route: String,
    val title: String,
    val selectedIcon: ImageVector,
    val unselectedIcon: ImageVector
) {
    data object Search : Screen("search", "Search", Icons.Filled.Search, Icons.Outlined.Search)
    data object History : Screen("history", "History", Icons.Filled.History, Icons.Outlined.History)
    data object Downloads : Screen("downloads", "Downloads", Icons.Filled.Download, Icons.Outlined.Download)
    data object Settings : Screen("settings", "Settings", Icons.Filled.Settings, Icons.Filled.Settings)
}

private val bottomNavScreens = listOf(Screen.Search, Screen.History, Screen.Downloads)

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun CineIndexNavHost() {
    val navController = rememberNavController()
    val navBackStackEntry by navController.currentBackStackEntryAsState()
    val currentDestination = navBackStackEntry?.destination
    val currentRoute = currentDestination?.route

    // Hide chrome on settings and player screens
    val showBottomBar = currentRoute in bottomNavScreens.map { it.route }

    Scaffold(
        topBar = {
            if (showBottomBar) {
                TopAppBar(
                    title = {
                        Text(
                            "CineIndex",
                            style = MaterialTheme.typography.headlineMedium,
                            color = MaterialTheme.colorScheme.primary
                        )
                    },
                    actions = {
                        IconButton(onClick = { navController.navigate(Screen.Settings.route) }) {
                            Icon(
                                Icons.Filled.Settings,
                                contentDescription = "Settings",
                                tint = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                    },
                    colors = TopAppBarDefaults.topAppBarColors(
                        containerColor = MaterialTheme.colorScheme.surface
                    )
                )
            }
        },
        bottomBar = {
            if (showBottomBar) {
                NavigationBar(
                    containerColor = MaterialTheme.colorScheme.surfaceContainer
                ) {
                    bottomNavScreens.forEach { screen ->
                        val selected = currentDestination?.hierarchy?.any { it.route == screen.route } == true
                        NavigationBarItem(
                            icon = {
                                Icon(
                                    if (selected) screen.selectedIcon else screen.unselectedIcon,
                                    contentDescription = screen.title
                                )
                            },
                            label = { Text(screen.title) },
                            selected = selected,
                            onClick = {
                                navController.navigate(screen.route) {
                                    popUpTo(navController.graph.findStartDestination().id) {
                                        saveState = true
                                    }
                                    launchSingleTop = true
                                    restoreState = true
                                }
                            }
                        )
                    }
                }
            }
        }
    ) { innerPadding ->
        NavHost(
            navController = navController,
            startDestination = Screen.Search.route,
            modifier = Modifier.fillMaxSize()
        ) {
            composable(Screen.Search.route) {
                Box(modifier = Modifier.padding(innerPadding)) { SearchScreen() }
            }
            composable(Screen.History.route) {
                Box(modifier = Modifier.padding(innerPadding)) { HistoryScreen() }
            }
            composable(Screen.Downloads.route) {
                Box(modifier = Modifier.padding(innerPadding)) { DownloadsScreen() }
            }
            composable(Screen.Settings.route) { 
                // Settings uses its own Scaffold so it handles its own insets
                SettingsScreen(onBack = { navController.popBackStack() }) 
            }
        }
    }
}
