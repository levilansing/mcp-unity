using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using UnityEditor;
using UnityEngine;
using McpUnity.Tools;
using McpUnity.Resources;
using McpUnity.Services;
using McpUnity.Utils;
using WebSocketSharp.Server;
using System.IO;
using System.Net.Sockets;
using UnityEditor.Callbacks;

namespace McpUnity.Unity
{
    /// <summary>
    /// Custom WebSocket close codes for Unity-specific events.
    /// Range 4000-4999 is reserved for application use.
    /// </summary>
    public static class UnityCloseCode
    {
        /// <summary>
        /// Unity is entering Play mode - clients should use fast polling instead of backoff
        /// </summary>
        public const ushort PlayMode = 4001;
    }

    /// <summary>
    /// MCP Unity Server to communicate Node.js MCP server.
    /// Uses WebSockets to communicate with Node.js.
    /// </summary>
    [InitializeOnLoad]
    public class McpUnityServer : IDisposable
    {
        private static McpUnityServer _instance;

        private readonly Dictionary<string, McpToolBase> _tools = new Dictionary<string, McpToolBase>();
        private readonly Dictionary<string, McpResourceBase> _resources = new Dictionary<string, McpResourceBase>();

        private WebSocketServer _webSocketServer;
        private CancellationTokenSource _cts;
        private TestRunnerService _testRunnerService;
        private ConsoleLogsService _consoleLogsService;

        /// <summary>
        /// Static constructor invoked by [InitializeOnLoad] on every domain load — including
        /// play-mode domain reloads, which [DidReloadScripts] does NOT cover. Re-creating the
        /// instance here re-subscribes the editor lifecycle handlers that a domain reload wipes,
        /// so the server is reliably stopped before, and restarted after, every reload instead
        /// of orphaning its listening socket and leaking the port until the editor is restarted.
        /// </summary>
        static McpUnityServer()
        {
            // Skip in batch mode (Unity Cloud Build, CI, headless builds) to avoid hanging npm.
            if (Application.isBatchMode)
            {
                return;
            }

            // Defer until the reload settles; touching Instance recreates the singleton and
            // re-subscribes its event handlers (and starts the server when appropriate).
            EditorApplication.delayCall += EnsureInitialized;
        }

        /// <summary>
        /// Ensures the singleton exists and its editor lifecycle hooks are subscribed.
        /// Safe to call repeatedly; instance creation is idempotent.
        /// </summary>
        private static void EnsureInitialized()
        {
            if (Application.isBatchMode)
            {
                return;
            }

            // Touching Instance creates the singleton (if needed) and subscribes its lifecycle hooks.
            var _ = Instance;
        }

        /// <summary>
        /// Called after every script-compilation domain reload
        /// </summary>
        [DidReloadScripts]
        private static void AfterReload()
        {
            // Skip initialization in batch mode (Unity Cloud Build, CI, headless builds)
            // This prevents npm commands from hanging the build process
            EnsureInitialized();
        }
        
        /// <summary>
        /// Singleton instance accessor. Returns null in batch mode.
        /// </summary>
        public static McpUnityServer Instance
        {
            get
            {
                // Don't create instance in batch mode to avoid hanging builds
                if (Application.isBatchMode)
                {
                    return null;
                }
                
                if (_instance == null)
                {
                    _instance = new McpUnityServer();
                }
                return _instance;
            }
        }

        /// <summary>
        /// Current Listening state
        /// </summary>
        public bool IsListening => _webSocketServer?.IsListening ?? false;

        /// <summary>
        /// Thread-safe dictionary of connected clients with this server.
        /// WebSocketSharp dispatches OnOpen/OnClose on thread pool threads,
        /// so concurrent access must be safe.
        /// </summary>
        public ConcurrentDictionary<string, string> Clients { get; } = new ConcurrentDictionary<string, string>();

        /// <summary>
        /// Private constructor to enforce singleton pattern
        /// </summary>
        private McpUnityServer()
        {
            // Skip all initialization in batch mode (Unity Cloud Build, CI, headless builds)
            // The npm install/build commands can hang indefinitely without node.js available
            if (Application.isBatchMode)
            {
                McpLogger.LogInfo("MCP Unity server disabled: Running in batch mode (Unity Cloud Build or CI)");
                return;
            }
            
            EditorApplication.quitting -= OnEditorQuitting; // Prevent multiple subscriptions on domain reload
            EditorApplication.quitting += OnEditorQuitting;

            AssemblyReloadEvents.beforeAssemblyReload -= OnBeforeAssemblyReload;
            AssemblyReloadEvents.beforeAssemblyReload += OnBeforeAssemblyReload;

            AssemblyReloadEvents.afterAssemblyReload -= OnAfterAssemblyReload;
            AssemblyReloadEvents.afterAssemblyReload += OnAfterAssemblyReload;

            EditorApplication.playModeStateChanged -= OnPlayModeStateChanged;
            EditorApplication.playModeStateChanged += OnPlayModeStateChanged;

            InstallServer();
            InitializeServices();
            RegisterResources();
            RegisterTools();

            // Initial start if auto-start is enabled. Skip while entering/in Play Mode: a domain
            // reload during the play transition can orphan the listener socket. The server is
            // restarted on EnteredEditMode instead (see OnPlayModeStateChanged).
            if (McpUnitySettings.Instance.AutoStartServer && !EditorApplication.isPlayingOrWillChangePlaymode)
            {
                 StartServer();
            }
        }

        /// <summary>
        /// Disposes the McpUnityServer instance, stopping the WebSocket server and unsubscribing from Unity Editor events.
        /// This method ensures proper cleanup of resources and prevents memory leaks or unexpected behavior during domain reloads or editor shutdown.
        /// </summary>
        public void Dispose()
        {
            StopServer();

            EditorApplication.quitting -= OnEditorQuitting;
            AssemblyReloadEvents.beforeAssemblyReload -= OnBeforeAssemblyReload;
            AssemblyReloadEvents.afterAssemblyReload -= OnAfterAssemblyReload;
            EditorApplication.playModeStateChanged -= OnPlayModeStateChanged;

            GC.SuppressFinalize(this);
        }
        
        /// <summary>
        /// Start the WebSocket Server to communicate with Node.js
        /// </summary>
        public void StartServer()
        {
            StartServer(false);
        }

        private void StartServer(bool isRetry)
        {
            // Skip starting server if this is a Multiplayer Play Mode clone instance
            // Only the main editor should run the WebSocket server to avoid port conflicts
            if (McpUtils.IsMultiplayerPlayModeClone())
            {
                McpLogger.LogInfo("Server startup skipped: Running as Multiplayer Play Mode clone instance. Only the main editor runs the MCP server.");
                return;
            }

            if (IsListening)
            {
                McpLogger.LogInfo($"Server start requested, but already listening on port {McpUnitySettings.Instance.Port}.");
                return;
            }

            try
            {
                var host = McpUnitySettings.Instance.AllowRemoteConnections ? "0.0.0.0" : "localhost";
                _webSocketServer = new WebSocketServer($"ws://{host}:{McpUnitySettings.Instance.Port}");
                // NOTE: deliberately NOT setting ReuseAddress. On Windows SO_REUSEADDR lets a new
                // socket bind a port another socket is actively listening on, which would let us
                // bind alongside a leaked "ghost" listener (two servers, intermittent 501s) and
                // mask the real failure. A clean AddressAlreadyInUse is the signal we want.
                _webSocketServer.AddWebSocketService("/McpUnity", () => new McpUnitySocketHandler(this));
                _webSocketServer.Start();
                McpLogger.LogInfo($"WebSocket server started successfully on {host}:{McpUnitySettings.Instance.Port}.");
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.AddressAlreadyInUse)
            {
                // A listener from a previous domain/instance may still hold the port. Release the
                // partially-created server and retry once before surfacing the error to the user.
                if (!isRetry)
                {
                    McpLogger.LogWarning($"Port {McpUnitySettings.Instance.Port} is already in use; releasing any stale listener and retrying once...");
                    StopServer();
                    StartServer(true);
                    return;
                }

                McpLogger.LogError($"Failed to start WebSocket server: Port {McpUnitySettings.Instance.Port} is already in use. " +
                    $"A previous server instance may still hold the port — restart the Unity Editor if this persists. {ex.Message}");
            }
            catch (Exception ex)
            {
                McpLogger.LogError($"Failed to start WebSocket server: {ex.Message}\n{ex.StackTrace}");
            }
        }
        
        /// <summary>
        /// Stop the WebSocket server
        /// </summary>
        /// <param name="closeCode">Optional custom close code to send to clients before stopping</param>
        /// <param name="closeReason">Optional reason message for the close</param>
        public void StopServer(ushort? closeCode = null, string closeReason = null)
        {
            // Tear down whenever a server object exists, even if it is no longer listening
            // (e.g. a half-initialized server from a failed Start) so its socket is released.
            if (_webSocketServer == null)
            {
                return;
            }

            try
            {
                // If a custom close code is provided, close all client connections with that code first
                if (closeCode.HasValue)
                {
                    CloseAllClients(closeCode.Value, closeReason ?? "Server stopping");
                }

                // websocket-sharp's Stop() does NOT reliably close the underlying listener socket:
                // its accept thread stays blocked on the port, and a domain reload then orphans that
                // thread, leaking the port until the process dies. Close the socket ourselves —
                // while we still hold the reference — so it is released synchronously.
                ForceCloseUnderlyingListener(_webSocketServer);

                try
                {
                    _webSocketServer.Stop();
                }
                catch
                {
                    // Stop() can throw because the listener was already closed above; the socket is
                    // released regardless, so this is safe to ignore.
                }

                McpLogger.LogInfo("WebSocket server stopped");
            }
            catch (Exception ex)
            {
                McpLogger.LogError($"Error stopping WebSocket server: {ex.Message}\n{ex.StackTrace}");
            }
            finally
            {
                _webSocketServer = null;
                Clients.Clear();
            }
        }

        /// <summary>
        /// Forcibly closes the TcpListener/Socket that websocket-sharp's WebSocketServer holds
        /// internally. Its Stop() does not reliably release the listening socket, so we close it
        /// directly (via reflection) to unblock the accept thread and free the port synchronously,
        /// before a domain reload can orphan the thread. Walks the type hierarchy and closes every
        /// TcpListener/Socket field it finds.
        /// </summary>
        private static void ForceCloseUnderlyingListener(WebSocketServer server)
        {
            if (server == null) return;

            try
            {
                for (var type = server.GetType(); type != null && type != typeof(object); type = type.BaseType)
                {
                    foreach (var field in type.GetFields(System.Reflection.BindingFlags.NonPublic
                                 | System.Reflection.BindingFlags.Public
                                 | System.Reflection.BindingFlags.Instance
                                 | System.Reflection.BindingFlags.DeclaredOnly))
                    {
                        object value;
                        try { value = field.GetValue(server); }
                        catch { continue; }

                        if (value is System.Net.Sockets.TcpListener listener)
                        {
                            try { listener.Server?.Close(); } catch { }
                            try { listener.Stop(); } catch { }
                        }
                        else if (value is System.Net.Sockets.Socket socket)
                        {
                            try { socket.Close(); } catch { }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                McpLogger.LogWarning($"Failed to force-close WebSocket listener socket: {ex.Message}");
            }
        }

        /// <summary>
        /// Returns true if <paramref name="port"/> can be bound on IPv6 loopback (::1), which is
        /// the address websocket-sharp uses for "localhost" on Windows.
        /// </summary>
        private static bool IsPortFree(int port)
        {
            System.Net.Sockets.TcpListener probe = null;
            try
            {
                probe = new System.Net.Sockets.TcpListener(System.Net.IPAddress.IPv6Loopback, port);
                probe.Start();
                return true;
            }
            catch
            {
                return false;
            }
            finally
            {
                try { probe?.Stop(); } catch { }
            }
        }

        /// <summary>
        /// Stops the MCP WebSocket server (recovery control).
        /// Exposed as a menu item and a public static method so it can be invoked manually OR
        /// remotely via another working bridge (e.g. Unity's AI assistant `Unity_RunCommand`):
        ///   McpUnity.Unity.McpUnityServer.ForceStopServer();
        /// NOTE: this can only release the socket of the server this domain still holds a reference
        /// to. A listener orphaned by an earlier domain reload has no managed handle and CANNOT be
        /// freed in-process — only a Unity Editor restart releases it.
        /// </summary>
        [MenuItem("Tools/MCP Unity/Force Stop Server", false, 20)]
        public static void ForceStopServer()
        {
            if (Application.isBatchMode) return;

            Instance?.StopServer();

            int port = McpUnitySettings.Instance.Port;
            McpLogger.LogInfo($"Force stop requested. Port {port} free now: {IsPortFree(port)}.");
        }

        /// <summary>
        /// Stops and restarts the MCP WebSocket server (recovery control).
        /// Menu item + public static so it can be invoked manually OR remotely via another bridge:
        ///   McpUnity.Unity.McpUnityServer.ForceRestartServer();
        /// If the port is still held after the stop (an orphaned listener from a previous domain),
        /// the restart cannot bind and a Unity Editor restart is required — this reports that
        /// explicitly instead of silently failing.
        /// </summary>
        [MenuItem("Tools/MCP Unity/Force Restart Server", false, 21)]
        public static void ForceRestartServer()
        {
            if (Application.isBatchMode) return;

            var server = Instance;
            if (server == null) return;

            server.StopServer();

            int port = McpUnitySettings.Instance.Port;
            if (IsPortFree(port))
            {
                server.StartServer();
                McpLogger.LogInfo($"Force restart complete. Listening: {server.IsListening}.");
            }
            else
            {
                McpLogger.LogError($"Force restart: port {port} is still in use after stopping. This is " +
                    "almost certainly a listener orphaned by an earlier domain reload, which has no managed " +
                    "handle and cannot be released in-process. Restart the Unity Editor to free the port.");
            }
        }

        /// <summary>
        /// Close all connected clients with a specific close code
        /// </summary>
        /// <param name="closeCode">WebSocket close code (4000-4999 for application use)</param>
        /// <param name="reason">Reason message for the close</param>
        private void CloseAllClients(ushort closeCode, string reason)
        {
            if (_webSocketServer == null)
            {
                return;
            }

            try
            {
                var service = _webSocketServer.WebSocketServices["/McpUnity"];
                if (service?.Sessions != null)
                {
                    // Get all active session IDs and close each with the custom code
                    var sessionIds = new List<string>(service.Sessions.IDs);
                    foreach (var sessionId in sessionIds)
                    {
                        service.Sessions.CloseSession(sessionId, closeCode, reason);
                    }
                    McpLogger.LogInfo($"Closed {sessionIds.Count} client connection(s) with code {closeCode}: {reason}");
                }
            }
            catch (Exception ex)
            {
                McpLogger.LogError($"Error closing client connections: {ex.Message}");
            }
        }
        
        /// <summary>
        /// Try to get a tool by name
        /// </summary>
        public bool TryGetTool(string name, out McpToolBase tool)
        {
            return _tools.TryGetValue(name, out tool);
        }
        
        /// <summary>
        /// Try to get a resource by name
        /// </summary>
        public bool TryGetResource(string name, out McpResourceBase resource)
        {
            return _resources.TryGetValue(name, out resource);
        }

        /// <summary>
        /// Installs the MCP Node.js server by running 'npm install' and 'npm run build'
        /// in the server directory if 'node_modules' or 'build' folders are missing.
        /// </summary>
        public void InstallServer()
        {
            string serverPath = McpUtils.GetServerPath();

            if (string.IsNullOrEmpty(serverPath) || !Directory.Exists(serverPath))
            {
                McpLogger.LogError($"Server path not found or invalid: {serverPath}. Make sure that MCP Node.js server is installed.");
                return;
            }

            // Validate server path and warn about potential issues (spaces, special characters)
            if (!McpUtils.ValidateServerPath(serverPath))
            {
                McpLogger.LogError("Server path validation failed. See previous errors for details.");
                return;
            }

            string nodeModulesPath = Path.Combine(serverPath, "node_modules");
            if (!Directory.Exists(nodeModulesPath))
            {
                McpUtils.RunNpmCommand("install", serverPath);
            }

            string buildPath = Path.Combine(serverPath, "build");
            if (!Directory.Exists(buildPath))
            {
                McpUtils.RunNpmCommand("run build", serverPath);
            }
        }
        
        /// <summary>
        /// Register all available tools
        /// </summary>
        private void RegisterTools()
        {
            // Register MenuItemTool
            MenuItemTool menuItemTool = new MenuItemTool();
            _tools.Add(menuItemTool.Name, menuItemTool);
            
            // Register SelectGameObjectTool
            SelectGameObjectTool selectGameObjectTool = new SelectGameObjectTool();
            _tools.Add(selectGameObjectTool.Name, selectGameObjectTool);

            // Register UpdateGameObjectTool
            UpdateGameObjectTool updateGameObjectTool = new UpdateGameObjectTool();
            _tools.Add(updateGameObjectTool.Name, updateGameObjectTool);
            
            // Register PackageManagerTool
            AddPackageTool addPackageTool = new AddPackageTool();
            _tools.Add(addPackageTool.Name, addPackageTool);
            
            // Register RunTestsTool
            RunTestsTool runTestsTool = new RunTestsTool(_testRunnerService);
            _tools.Add(runTestsTool.Name, runTestsTool);
            
            // Register SendConsoleLogTool
            SendConsoleLogTool sendConsoleLogTool = new SendConsoleLogTool();
            _tools.Add(sendConsoleLogTool.Name, sendConsoleLogTool);
            
            // Register UpdateComponentTool
            UpdateComponentTool updateComponentTool = new UpdateComponentTool();
            _tools.Add(updateComponentTool.Name, updateComponentTool);
            
            // Register AddAssetToSceneTool
            AddAssetToSceneTool addAssetToSceneTool = new AddAssetToSceneTool();
            _tools.Add(addAssetToSceneTool.Name, addAssetToSceneTool);
            
            // Register CreatePrefabTool
            CreatePrefabTool createPrefabTool = new CreatePrefabTool();
            _tools.Add(createPrefabTool.Name, createPrefabTool);

            // Register CreateSceneTool
            CreateSceneTool createSceneTool = new CreateSceneTool();
            _tools.Add(createSceneTool.Name, createSceneTool);

            // Register DeleteSceneTool
            DeleteSceneTool deleteSceneTool = new DeleteSceneTool();
            _tools.Add(deleteSceneTool.Name, deleteSceneTool);

            // Register LoadSceneTool
            LoadSceneTool loadSceneTool = new LoadSceneTool();
            _tools.Add(loadSceneTool.Name, loadSceneTool);

            // Register SaveSceneTool
            SaveSceneTool saveSceneTool = new SaveSceneTool();
            _tools.Add(saveSceneTool.Name, saveSceneTool);

            // Register GetSceneInfoTool
            GetSceneInfoTool getSceneInfoTool = new GetSceneInfoTool();
            _tools.Add(getSceneInfoTool.Name, getSceneInfoTool);

            // Register UnloadSceneTool
            UnloadSceneTool unloadSceneTool = new UnloadSceneTool();
            _tools.Add(unloadSceneTool.Name, unloadSceneTool);

            // Register RecompileScriptsTool
            RecompileScriptsTool recompileScriptsTool = new RecompileScriptsTool();
            _tools.Add(recompileScriptsTool.Name, recompileScriptsTool);
            
            // Register GetGameObjectTool
            GetGameObjectTool getGameObjectTool = new GetGameObjectTool();
            _tools.Add(getGameObjectTool.Name, getGameObjectTool);

            // Register DuplicateGameObjectTool
            DuplicateGameObjectTool duplicateGameObjectTool = new DuplicateGameObjectTool();
            _tools.Add(duplicateGameObjectTool.Name, duplicateGameObjectTool);

            // Register DeleteGameObjectTool
            DeleteGameObjectTool deleteGameObjectTool = new DeleteGameObjectTool();
            _tools.Add(deleteGameObjectTool.Name, deleteGameObjectTool);

            // Register ReparentGameObjectTool
            ReparentGameObjectTool reparentGameObjectTool = new ReparentGameObjectTool();
            _tools.Add(reparentGameObjectTool.Name, reparentGameObjectTool);

            // Register Transform Tools
            MoveGameObjectTool moveGameObjectTool = new MoveGameObjectTool();
            _tools.Add(moveGameObjectTool.Name, moveGameObjectTool);

            RotateGameObjectTool rotateGameObjectTool = new RotateGameObjectTool();
            _tools.Add(rotateGameObjectTool.Name, rotateGameObjectTool);

            ScaleGameObjectTool scaleGameObjectTool = new ScaleGameObjectTool();
            _tools.Add(scaleGameObjectTool.Name, scaleGameObjectTool);

            SetTransformTool setTransformTool = new SetTransformTool();
            _tools.Add(setTransformTool.Name, setTransformTool);

            // Register Material Tools
            CreateMaterialTool createMaterialTool = new CreateMaterialTool();
            _tools.Add(createMaterialTool.Name, createMaterialTool);

            AssignMaterialTool assignMaterialTool = new AssignMaterialTool();
            _tools.Add(assignMaterialTool.Name, assignMaterialTool);

            ModifyMaterialTool modifyMaterialTool = new ModifyMaterialTool();
            _tools.Add(modifyMaterialTool.Name, modifyMaterialTool);

            GetMaterialInfoTool getMaterialInfoTool = new GetMaterialInfoTool();
            _tools.Add(getMaterialInfoTool.Name, getMaterialInfoTool);

            // Register BatchExecuteTool (must be registered last as it needs access to other tools)
            BatchExecuteTool batchExecuteTool = new BatchExecuteTool(this);
            _tools.Add(batchExecuteTool.Name, batchExecuteTool);
        }
        
        /// <summary>
        /// Register all available resources
        /// </summary>
        private void RegisterResources()
        {
            // Register GetMenuItemsResource
            GetMenuItemsResource getMenuItemsResource = new GetMenuItemsResource();
            _resources.Add(getMenuItemsResource.Name, getMenuItemsResource);
            
            // Register GetConsoleLogsResource
            GetConsoleLogsResource getConsoleLogsResource = new GetConsoleLogsResource(_consoleLogsService);
            _resources.Add(getConsoleLogsResource.Name, getConsoleLogsResource);
            
            // Register GetScenesHierarchyResource
            GetScenesHierarchyResource getScenesHierarchyResource = new GetScenesHierarchyResource();
            _resources.Add(getScenesHierarchyResource.Name, getScenesHierarchyResource);
            
            // Register GetPackagesResource
            GetPackagesResource getPackagesResource = new GetPackagesResource();
            _resources.Add(getPackagesResource.Name, getPackagesResource);
            
            // Register GetAssetsResource
            GetAssetsResource getAssetsResource = new GetAssetsResource();
            _resources.Add(getAssetsResource.Name, getAssetsResource);
            
            // Register GetTestsResource
            GetTestsResource getTestsResource = new GetTestsResource(_testRunnerService);
            _resources.Add(getTestsResource.Name, getTestsResource);
            
            // Register GetGameObjectResource
            GetGameObjectResource getGameObjectResource = new GetGameObjectResource();
            _resources.Add(getGameObjectResource.Name, getGameObjectResource);
        }
        
        /// <summary>
        /// Initialize services used by the server
        /// </summary>
        private void InitializeServices()
        {
            // Initialize the test runner service
            _testRunnerService = new TestRunnerService();
            
            // Initialize the console logs service
            _consoleLogsService = new ConsoleLogsService();
        }

        /// <summary>
        /// Handles the Unity Editor quitting event. Ensures the server is properly stopped and disposed.
        /// </summary>
        private static void OnEditorQuitting()
        {
            if (Application.isBatchMode || _instance == null) return;
            
            McpLogger.LogInfo("Editor is quitting. Ensuring server is stopped.");
            _instance.Dispose();
        }

        /// <summary>
        /// Handles the Unity Editor's 'before assembly reload' event.
        /// Stops the WebSocket server to prevent port conflicts and ensure a clean state before scripts are recompiled.
        /// </summary>
        private static void OnBeforeAssemblyReload()
        {
            if (Application.isBatchMode || _instance == null) return;

            if (_instance.IsListening)
            {
                _instance.StopServer();
            }
        }

        /// <summary>
        /// Handles the Unity Editor's 'after assembly reload' event.
        /// If auto-start is enabled, attempts to restart the WebSocket server if it's not already listening.
        /// This ensures the server is operational after script recompilation.
        /// </summary>
        private static void OnAfterAssemblyReload()
        {
            if (Application.isBatchMode || _instance == null) return;

            // Don't restart while in Play Mode; EnteredEditMode handles the edit-mode restart.
            if (McpUnitySettings.Instance.AutoStartServer && !_instance.IsListening && !EditorApplication.isPlaying)
            {
                _instance.StartServer();
            }
        }

        /// <summary>
        /// Handles changes in Unity Editor's play mode state.
        /// Stops the server when exiting Edit Mode if configured, and restarts it when entering Play Mode or returning to Edit Mode if auto-start is enabled.
        /// </summary>
        /// <param name="state">The current play mode state change.</param>
        private static void OnPlayModeStateChanged(PlayModeStateChange state)
        {
            if (Application.isBatchMode || _instance == null) return;

            switch (state)
            {
                case PlayModeStateChange.ExitingEditMode:
                    // About to enter Play Mode - use custom close code so clients use fast polling
                    if (_instance.IsListening)
                    {
                        _instance.StopServer(UnityCloseCode.PlayMode, "Unity entering Play mode");
                    }
                    break;
                case PlayModeStateChange.EnteredPlayMode:
                case PlayModeStateChange.ExitingPlayMode:
                    // Server is disabled during play mode as domain reload will be triggered again when stopped.
                    break;
                case PlayModeStateChange.EnteredEditMode:
                    // Returned to Edit Mode
                    if (!_instance.IsListening && McpUnitySettings.Instance.AutoStartServer)
                    {
                        _instance.StartServer();
                    }
                    break;
            }
        }
    }
}
