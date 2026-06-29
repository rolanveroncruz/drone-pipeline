package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/tus/tusd/v2/pkg/filestore"
	"github.com/tus/tusd/v2/pkg/handler"
)

const (
	webodmURL  = "http://localhost:8000"
	storageDir = "./local_storage"
	webodmUser = "admin" // Replace with your actual WebODM credentials
	webodmPass = "admin_password_here"
)

type TokenResponse struct {
	Token string `json:"token"`
}

func main() {
	// 1. Establish the Tus local chunk data-store partition
	store := filestore.New(storageDir)
	composer := handler.NewStoreComposer()
	store.UseIn(composer)

	tusHandler, err := handler.NewHandler(handler.Config{
		BasePath:              "/files/",
		StoreComposer:         composer,
		NotifyCompleteUploads: true,
	})
	if err != nil {
		fmt.Errorf("Failed to initialize Tus handler: %v", err)
		return
	}

	// 2. Spawn a background thread to intercept fully compiled uploads
	go handleCompletedUploads(tusHandler.CompleteUploads)

	// Routing setup
	http.Handle("/files/", http.StripPrefix("/files/", tusHandler))

	fmt.Println("🚀 Resumable Go Orchestrator running on http://localhost:8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("Server failure: %v\n", err)
	}
}

func handleCompletedUploads(completeChan chan handler.HookEvent) {
	for event := range completeChan {
		fileInfo := event.Upload
		fmt.Printf("💚 File upload finished via Tus: %s (%d bytes)\n", fileInfo.ID, fileInfo.Size)

		// Create a separate workspace for this target collection
		projectID := "proj_" + time.Now().Format("20060102_150405")
		projectPath := filepath.Join(storageDir, projectID)
		os.MkdirAll(projectPath, os.ModePerm)

		// Relocate the stitched chunk payload to our raw project space
		srcPath := filepath.Join(storageDir, fileInfo.ID)
		dstPath := filepath.Join(projectPath, fileInfo.MetaData["filename"])
		os.Rename(srcPath, dstPath)

		// Auto-trigger automation parsing path once full dataset arrives
		go processDataset(projectPath, projectID)
	}
}

func processDataset(dirPath, projectName string) {
	fmt.Printf("📦 Gathering dataset assets inside %s...\n", dirPath)

	// Scan directory for image files
	var images []string
	filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && (strings.HasSuffix(strings.ToLower(path), ".jpg") || strings.HasSuffix(strings.ToLower(path), ".jpeg")) {
			images = append(images, path)
		}
		return nil
	})

	if len(images) == 0 {
		fmt.Println("⚠️ Missing valid image payloads. Aborting.")
		return
	}

	zipPath := dirPath + ".zip"
	if err := createZip(zipPath, images); err != nil {
		fmt.Printf("❌ Zip archival compression error: %v\n", err)
		return
	}

	token, err := fetchWebODMToken()
	if err != nil {
		fmt.Printf("❌ Authentication handshake failed: %v\n", err)
		return
	}

	taskID, err := dispatchToWebODM(token, zipPath, projectName)
	if err != nil {
		fmt.Printf("❌ Pipeline orchestration submission failed: %v\n", err)
		return
	}

	fmt.Printf("🎉 Map pipeline initiated! Task Track ID: %s\n", taskID)
	monitorWebODMTask(token, taskID)
}

func createZip(zipPath string, files []string) error {
	zipFile, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	archive := zip.NewWriter(zipFile)
	defer archive.Close()

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		defer f.Close()

		w, err := archive.Create(filepath.Base(file))
		if err != nil {
			return err
		}
		if _, err := io.Copy(w, f); err != nil {
			return err
		}
	}
	return nil
}

func fetchWebODMToken() (string, error) {
	authURL := fmt.Sprintf("%s/api/token-auth/", webodmURL)
	payload, _ := json.Marshal(map[string]string{"username": webodmUser, "password": webodmPass})

	resp, err := http.Post(authURL, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var res TokenResponse
	json.NewDecoder(resp.Body).Decode(&res)
	return res.Token, nil
}

func dispatchToWebODM(token, zipPath, name string) (string, error) {
	taskURL := fmt.Sprintf("%s/api/projects/1/tasks/", webodmURL)
	file, err := os.Open(zipPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("images", filepath.Base(zipPath))
	if err != nil {
		return "", err
	}
	io.Copy(part, file)

	// WebODM formatting constraints require processing options arrays
	options, _ := json.Marshal([]map[string]interface{}{
		{"name": "dsm", "value": true},
		{"name": "orthophoto-resolution", "value": 5},
	})
	writer.WriteField("options", string(options))
	writer.WriteField("name", name)
	writer.Close()

	req, _ := http.NewRequest("POST", taskURL, body)
	req.Header.Set("Authorization", "JWT "+token)
	req.Header.Set("Content-Type", writer.FormDataContentType())

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)

	// Safely retrieve stringified numerical keys [replaces incorrect uint256 variable implementation]
	if id, exists := result["id"]; exists {
		return fmt.Sprintf("%.0f", id.(float64)), nil
	}
	return "", fmt.Errorf("unexpected submission feedback layout from WebODM")
}

func monitorWebODMTask(token, taskID string) {
	statusURL := fmt.Sprintf("%s/api/projects/1/tasks/%s/", webodmURL, taskID)
	client := &http.Client{}

	for {
		time.Sleep(15 * time.Second)
		req, _ := http.NewRequest("GET", statusURL, nil)
		req.Header.Set("Authorization", "JWT "+token)

		resp, err := client.Do(req)
		if err != nil {
			continue
		}

		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()

		statusMap, ok := result["status"].(map[string]interface{})
		if !ok {
			continue
		}

		statusCode := fmt.Sprintf("%v", statusMap["code"])
		fmt.Printf("⏳ Native Polling Engine - Task %s Status: %s\n", taskID, statusCode)

		if statusCode == "COMPLETED" {
			fmt.Printf("💚 Photogrammetry Completed! Outputs stored locally under WebODM mount.\n")
			break
		} else if statusCode == "FAILED" {
			fmt.Printf("❌ Processing node encountered structural failure inside Docker.\n")
			break
		}
	}
}
