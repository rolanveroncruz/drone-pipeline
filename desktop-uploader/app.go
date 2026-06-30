package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/eventials/go-tus"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// App struct
type App struct {
	ctx context.Context
}

// NewApp creates a new App application struct
func NewApp() *App {
	return &App{}
}

// startup is called when the app starts. The context is saved
// so we can call the runtime methods
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
}

// SelectDroneFolder opens a native OS dialog and returns absolute paths of valid images
func (a *App) SelectDroneFolder() ([]string, error) {
	// 1. Trigger the native platform directory dialog
	targetDir, err := runtime.OpenDirectoryDialog(a.ctx, runtime.OpenDialogOptions{
		Title: "Select Drone SD Card or Mission Folder",
	})
	if err != nil {
		return nil, err
	}

	// If the user cancelled the selection, return an empty slice
	if targetDir == "" {
		return []string{}, nil
	}

	var imagePaths []string

	// 2. Efficiently walk the directory to find raw imagery
	err = filepath.Walk(targetDir,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// Skip directories
			if info.IsDir() {
				return nil
			}

			// Look for standard drone image extensions
			ext := strings.ToLower(filepath.Ext(path))
			if ext == ".jpg" || ext == ".jpeg" {
				imagePaths = append(imagePaths, path)
			}
			return nil
		})

	if err != nil {
		return nil, fmt.Errorf("failed to scan folder: %w", err)
	}

	return imagePaths, nil
}

// UploadFolderToOrchestrator takes the paths from the UI and pumps them to the orchestrator via Go
func (a *App) UploadFolderToOrchestrator(filePaths []string) error {
	orchestratorURL := "http://localhost:8080/files/"

	config := tus.DefaultConfig()
	config.ChunkSize = 2 * 1024 * 1024

	client, err := tus.NewClient(orchestratorURL, config)
	if err != nil {
		return fmt.Errorf("failed to configure tus client: %w", err)
	}

	totalFiles := len(filePaths)

	for index, path := range filePaths {
		err := func() error {
			file, err := os.Open(path)
			if err != nil {
				return nil
			}
			defer file.Close()

			fileName := filepath.Base(path)

			upload, err := tus.NewUploadFromFile(file)
			if err != nil {
				return fmt.Errorf("failed to read file structure: %w", err)
			}

			// 💡 Fix: Explicitly base64 encode strings to adhere to Tus metadata specifications
			encodedName := base64.StdEncoding.EncodeToString([]byte(fileName))
			encodedPath := base64.StdEncoding.EncodeToString([]byte(path))

			upload.Metadata = tus.Metadata{
				"filename": encodedName,
				"absolute": encodedPath,
			}

			uploader, err := client.CreateUpload(upload)
			if err != nil {
				return fmt.Errorf("orchestrator handshake failed on %s: %w", fileName, err)
			}

			runtime.EventsEmit(a.ctx, "next-file-starting", map[string]interface{}{
				"name":  fileName,
				"index": index + 1,
			})

			progressChan := make(chan tus.Upload, 1)

			go func() {
				for status := range progressChan {
					fileProgress := status.Progress()
					overallProgress := (float64(index)/float64(totalFiles))*100 + (float64(fileProgress) / float64(totalFiles))

					runtime.EventsEmit(a.ctx, "upload-metrics", map[string]interface{}{
						"fileProgress":    fileProgress,
						"overallProgress": int(overallProgress + 0.5),
					})
				}
			}()

			uploader.NotifyUploadProgress(progressChan)

			err = uploader.Upload()
			if err != nil {
				return fmt.Errorf("stream broke on file %s: %w", fileName, err)
			}

			return nil
		}()

		if err != nil {
			return err
		}
	}

	runtime.EventsEmit(a.ctx, "upload-complete", true)
	return nil
}
