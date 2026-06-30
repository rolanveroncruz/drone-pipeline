package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tus/tusd/v2/pkg/filestore"
	"github.com/tus/tusd/v2/pkg/handler"
)

const (
	webodmURL  = "http://localhost:8000"
	storageDir = "./local_storage"
	outputDir  = "./completed_results"
	webodmUser = "rolanveroncruz"
	webodmPass = "asdf"
)

type TokenResponse struct {
	Token string `json:"token"`
}

type TusUploadInfo struct {
	MetaData map[string]string `json:"metadata"`
}

type TaskResponse struct {
	ID              interface{} `json:"id"`
	UUID            string      `json:"uuid"`
	Status          int         `json:"status"`
	LastError       string      `json:"last_error"`
	AvailableAssets []string    `json:"available_assets"`
}

var (
	watchdogTimer *time.Timer
	pipelineMutex sync.Mutex
	quietPeriod   = 4 * time.Second
)

func main() {
	os.MkdirAll(storageDir, 0755)
	os.MkdirAll(outputDir, 0755)

	store := filestore.New(storageDir)
	composer := handler.NewStoreComposer()
	store.UseIn(composer)

	tusHandler, err := handler.NewHandler(handler.Config{
		BasePath:              "/files/",
		StoreComposer:         composer,
		NotifyCompleteUploads: true,
	})
	if err != nil {
		fmt.Printf("❌ Failed to initiate Tus loop: %v\n", err)
		return
	}

	go handleCompletedUploads(tusHandler.CompleteUploads)

	http.Handle("/files/", http.StripPrefix("/files/", tusHandler))

	fmt.Println("🚀 Orchestrator online at http://localhost:8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		fmt.Printf("❌ Server error: %v\n", err)
	}
}

func handleCompletedUploads(completeChan chan handler.HookEvent) {
	for event := range completeChan {
		fmt.Printf("💚 File secured: %s\n", event.Upload.ID)
		if watchdogTimer != nil {
			watchdogTimer.Stop()
		}
		watchdogTimer = time.AfterFunc(quietPeriod, func() {
			projectName := fmt.Sprintf("Flight_%s", time.Now().Format("20060102_150405"))
			go processDataset(storageDir, projectName)
		})
	}
}

func processDataset(dirPath, projectName string) {
	pipelineMutex.Lock()
	defer pipelineMutex.Unlock()

	stagingDir := filepath.Join(os.TempDir(), projectName+"_staging")
	os.MkdirAll(stagingDir, 0755)
	defer os.RemoveAll(stagingDir)

	entries, _ := os.ReadDir(dirPath)
	var isolatedImages []string

	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), ".info") {
			continue
		}
		basePath := filepath.Join(dirPath, entry.Name())
		infoPath := basePath + ".info"
		fi, err := os.Stat(basePath)
		if err != nil || fi.Size() == 0 {
			continue
		}

		infoBytes, _ := os.ReadFile(infoPath)
		var meta TusUploadInfo
		json.Unmarshal(infoBytes, &meta)

		targetedName := entry.Name() + ".jpg"
		if encoded, exists := meta.MetaData["filename"]; exists {
			if decoded, err := base64.StdEncoding.DecodeString(encoded); err == nil {
				targetedName = string(decoded)
			}
		}

		ext := filepath.Ext(targetedName)
		if strings.ToLower(ext) == ".jpg" || strings.ToLower(ext) == ".jpeg" {
			nameWithoutExt := strings.TrimSuffix(targetedName, ext)
			targetedName = nameWithoutExt + ".jpg"
		}

		dest := filepath.Join(stagingDir, targetedName)
		copyFile(basePath, dest)
		isolatedImages = append(isolatedImages, dest)
		os.Remove(basePath)
		os.Remove(infoPath)
	}

	if len(isolatedImages) < 2 {
		return
	}

	token, err := fetchWebODMToken()
	if err != nil {
		fmt.Println("❌ Auth failed:", err)
		return
	}

	taskID, err := dispatchLooseImagesToWebODM(isolatedImages, projectName, token)
	if err == nil {
		fmt.Printf("💚 Task %v registered. Watching status...\n", taskID)
		go monitorTask(fmt.Sprintf("%v", taskID), token)
	}
}

func monitorTask(taskID string, token string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		url := fmt.Sprintf("%s/api/projects/1/tasks/%s/", webodmURL, taskID)
		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("Authorization", fmt.Sprintf("JWT %s", token))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			fmt.Println("⚠️ Polling error:", err)
			continue
		}

		// ✅ STRICT CHECK: Ensure response is 200 before decoding
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("⚠️ Polling error, status: %d\n", resp.StatusCode)
			resp.Body.Close()
			continue
		}

		var task TaskResponse
		if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
			fmt.Println("❌ JSON decode error:", err)
		}
		resp.Body.Close()

		if task.Status == 0 {
			fmt.Printf("⏳ Task %v is initializing...\n", taskID)
			continue
		}

		fmt.Printf("📊 Task %v status: %d\n", taskID, task.Status)

		switch task.Status {
		case 30:
			fmt.Println("❌ Task permanently failed.")
			if task.LastError != "" {
				fmt.Println(" Last error:", task.LastError)
			}
			fetchAndPrintLogs(taskID, token)
			return

		case 40:
			fmt.Println("✅ Task completed! Downloading results...")
			downloadResult(taskID, token)
			return

		case 50:
			fmt.Println("Task was cancelled.")
			return
		}
	}
}

func fetchAndPrintLogs(taskID string, token string) {
	url := fmt.Sprintf("%s/api/projects/1/tasks/%s/output/?line=0", webodmURL, taskID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println("Failed to build output request:", err)
	}
	req.Header.Set("Authorization", fmt.Sprintf("JWT %s", token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println("Failed to fetch task output:", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Printf("output fetch failed. Status: %d\n%s\n", resp.StatusCode, string(body))
		return
	}
	fmt.Printf("WebODM Task Output:\n%s\n", string(body))
}

func downloadResult(taskID string, token string) {
	url := fmt.Sprintf("%s/api/projects/1/tasks/%s/download/orthophoto.tif", webodmURL, taskID)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", fmt.Sprintf("JWT %s", token))

	resp, _ := http.DefaultClient.Do(req)
	if resp.StatusCode == 200 {
		defer resp.Body.Close()
		out, _ := os.Create(filepath.Join(outputDir, fmt.Sprintf("Orthomosaic_%s.tif", taskID)))
		io.Copy(out, resp.Body)
		out.Close()
		fmt.Println("🎉 Final .tif map saved to ./completed_results/")
	}
}

func dispatchLooseImagesToWebODM(imagePaths []string, projectName string, token string) (interface{}, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	_ = writer.WriteField("name", projectName)
	_ = writer.WriteField("options", "[]")

	for _, path := range imagePaths {
		file, _ := os.Open(path)
		part, _ := writer.CreateFormFile("images", filepath.Base(path))
		io.Copy(part, file)
		file.Close()
	}
	writer.Close()

	req, _ := http.NewRequest("POST", fmt.Sprintf("%s/api/projects/1/tasks/", webodmURL), body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Authorization", fmt.Sprintf("JWT %s", token))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server rejected: %s", string(body))
	}

	var taskRes TaskResponse
	json.NewDecoder(resp.Body).Decode(&taskRes)
	return taskRes.ID, nil
}

func fetchWebODMToken() (string, error) {
	payload, _ := json.Marshal(map[string]string{"username": webodmUser, "password": webodmPass})
	resp, err := http.Post(fmt.Sprintf("%s/api/token-auth/", webodmURL), "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("auth error: %d", resp.StatusCode)
	}
	var tr TokenResponse
	json.NewDecoder(resp.Body).Decode(&tr)
	return tr.Token, nil
}

func copyFile(src, dst string) error {
	in, _ := os.Open(src)
	defer in.Close()
	out, _ := os.Create(dst)
	defer out.Close()
	_, err := io.Copy(out, in)
	return err
}
