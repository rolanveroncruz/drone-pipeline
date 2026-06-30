<script lang="ts">
    import { onMount } from 'svelte';
    import { SelectDroneFolder, UploadFolderToOrchestrator } from '../wailsjs/go/main/App';

    // Bring in native Wails runtime hooks for event parsing
    import { EventsOn } from '../wailsjs/runtime';

    // State Management
    let selectedFiles: string[] = [];
    let errorMessage: string = "";
    let isScanning: boolean = false;
    let isUploading: boolean = false;

    // Real-time Ingestion Progress States (Updated from Go Runtime Events)
    let currentFileIndex: number = 0;
    let currentFileName: string = "";
    let fileProgress: number = 0;
    let overallProgress: number = 0;

    onMount(() => {
        // 1. Hook into Go's loop iteration change
        EventsOn("next-file-starting", (data: { name: string; index: number }) => {
            currentFileName = data.name;
            currentFileIndex = data.index;
            fileProgress = 0; // reset individual asset ticker
        });

        // 2. Consume dynamic byte metrics streamed out of the active Tus upload channel
        EventsOn("upload-metrics", (data: { fileProgress: number; overallProgress: number }) => {
            fileProgress = data.fileProgress;
            overallProgress = data.overallProgress;
        });

        // 3. Clear the pipeline state upon successful completion
        EventsOn("upload-complete", () => {
            isUploading = false;
            currentFileName = "Dataset securely offloaded to local storage!";
            overallProgress = 100;
            fileProgress = 100;
        });
    });

    async function handleBrowse() {
        isScanning = true;
        errorMessage = "";
        overallProgress = 0;
        fileProgress = 0;
        currentFileName = "";

        try {
            const files = await SelectDroneFolder();
            if (files && files.length > 0) {
                selectedFiles = files;
            } else {
                selectedFiles = [];
            }
        } catch (err) {
            errorMessage = "Failed to parse system directory paths.";
            console.error(err);
        } finally {
            isScanning = false;
        }
    }

    async function triggerUpload() {
        if (selectedFiles.length === 0) return;
        isUploading = true;
        errorMessage = "";

        try {
            // Pass the array down to the Go binary and release native performance
            await UploadFolderToOrchestrator(selectedFiles);
        } catch (err) {
            errorMessage = `Ingestion error: ${err}`;
            isUploading = false;
        }
    }
</script>

<main class="cockpit-container">
    <header class="app-header">
        <h1>Drone Data Pipeline</h1>
        <p class="subtitle">Go Native Ingestion Engine</p>
    </header>

    <section class="control-panel">
        <button class="btn btn-primary" on:click={handleBrowse} disabled={isScanning || isUploading}>
            {isScanning ? "Scanning Folder..." : "📁 Browse SD Card Folder"}
        </button>

        {#if selectedFiles.length > 0 && !isUploading && overallProgress < 100}
            <button class="btn btn-success" on:click={triggerUpload}>
                🚀 Stream {selectedFiles.length} Images via Go Native
            </button>
        {/if}
    </section>

    {#if isUploading || overallProgress > 0}
        <section class="progress-card">
            <div class="progress-meta">
                <strong>{isUploading ? "Streaming via Go Engine:" : "Pipeline Status:"}</strong>
                <span class="file-name-trim">{currentFileName}</span>
                <span class="percentage accent-blue">{overallProgress}% Overall</span>
            </div>

            <div class="progress-bar-bg">
                <div class="progress-bar-fill fill-overall" style="width: {overallProgress}%"></div>
            </div>

            <div class="file-meta-sub">
                <span>Asset {currentFileIndex} of {selectedFiles.length}</span>
                <span>Active File: {fileProgress}%</span>
            </div>
            <div class="progress-bar-bg sub-bar">
                <div class="progress-bar-fill fill-file" style="width: {fileProgress}%"></div>
            </div>
        </section>
    {/if}

    {#if errorMessage}
        <div class="alert error">{errorMessage}</div>
    {/if}

    <section class="file-manifest">
        <h3>Selected Flight Dataset Manifest</h3>
        {#if selectedFiles.length === 0}
            <p class="empty-state">No flight paths targeted. Insert a pilot SD card or select a subdirectory to parse image vectors.</p>
        {:else}
            <div class="scroll-container">
                <ul>
                    {#each selectedFiles as path}
                        <li class={path.includes(currentFileName) && isUploading ? "active-row" : ""}>{path}</li>
                    {/each}
                </ul>
            </div>
        {/if}
    </section>
</main>

<style>
    :global(body) {
        margin: 0;
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
        background-color: #1a1a24;
        color: #e2e8f0;
        user-select: none;
    }
    .cockpit-container { padding: 2rem; max-width: 900px; margin: 0 auto; }
    .app-header h1 { margin: 0; font-size: 2.2rem; color: #6366f1; }
    .subtitle { margin: 0.2rem 0 1.5rem 0; color: #94a3b8; font-weight: 500; font-size: 0.95rem; text-transform: uppercase; letter-spacing: 0.05em;}
    .control-panel { display: flex; gap: 1rem; margin-bottom: 2rem; }

    .btn { padding: 0.75rem 1.5rem; font-size: 1rem; font-weight: 600; border: none; border-radius: 6px; cursor: pointer; transition: filter 0.2s; }
    .btn:hover { filter: brightness(1.1); }
    .btn-primary { background-color: #4f46e5; color: white; }
    .btn-success { background-color: #10b981; color: white; }
    .btn:disabled { background-color: #475569; cursor: not-allowed; filter: none; }

    .progress-card { background: #1e1b4b; border: 1px solid #4338ca; border-radius: 8px; padding: 1.2rem; margin-bottom: 1.5rem; }
    .progress-meta { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.6rem; font-size: 0.95rem; gap: 0.5rem; }
    .file-name-trim { font-family: monospace; color: #38bdf8; flex-grow: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 400px; }
    .percentage { font-weight: bold; font-family: monospace; }
    .accent-blue { color: #38bdf8; }

    .progress-bar-bg { background: #312e81; border-radius: 999px; height: 12px; overflow: hidden; margin-bottom: 0.5rem; }
    .sub-bar { height: 6px; }
    .progress-bar-fill { height: 100%; transition: width 0.1s ease; }
    .fill-overall { background: #10b981; }
    .fill-file { background: #38bdf8; }

    .file-meta-sub { display: flex; justify-content: space-between; font-size: 0.8rem; color: #a5b4fc; margin-top: 0.8rem; margin-bottom: 0.3rem; font-family: monospace; }

    .alert { padding: 1rem; border-radius: 6px; margin-bottom: 1.5rem; font-family: monospace; }
    .error { background-color: #7f1d1d; color: #fca5a5; border: 1px solid #b91c1c; }

    .file-manifest { background-color: #11111b; border: 1px solid #334155; border-radius: 8px; padding: 1.5rem; }
    .scroll-container { max-height: 250px; overflow-y: auto; background: #09090e; border-radius: 4px; padding: 0.5rem; border: 1px solid #1e293b; }
    ul { list-style: none; padding: 0; margin: 0; }
    li { font-family: monospace; font-size: 0.85rem; padding: 0.4rem; border-bottom: 1px solid #1e293b; color: #64748b; word-break: break-all; }
    .empty-state { color: #64748b; font-style: italic; font-size: 0.9rem; }
</style>