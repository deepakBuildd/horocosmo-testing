package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	_ "github.com/lib/pq" // Postgres driver
	"github.com/sirupsen/logrus"
)

type BirthChart struct {
	ID   int    `json:"id"`
	Lat  string `json:"lat"`
	Lon  string `json:"lon"`
	Time string `json:"time"`
	Date string `json:"date"`
}

type GenerateResponse struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
	Data    struct {
		D1Rashi   []int    `json:"lalRashi"`
		D1Planets []string `json:"lalPlanets"`
	} `json:"data"`
}

type VerifyResponse struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
	Data    struct {
		Data []struct {
			Sign        int      `json:"sign"`
			SignName    string   `json:"sign_name"`
			PlanetSmall []string `json:"planet_small"`
		} `json:"data"`
	} `json:"data"`
}

func main() {
	// Connect to database
	db, err := sql.Open("postgres", "postgresql://horocosmo:horocosmo@3.111.219.25:5432/horocosmo?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	batchSize := 10 // Define the batch size
	// // Track batch number
	batch := 0

	// Fetch random entries
	entries, err := FetchRandomBirthCharts(db, batch*batchSize, batchSize)
	if err != nil {
		log.Fatalf("Failed to fetch random birth charts: %v", err)
	}

	// Read JSON file
	// data, err := os.ReadFile("batch3.json") // replace "data.json" with the path to your JSON file
	// if err != nil {
	// 	log.Fatalf("Failed to read file: %v", err)
	// }

	// // Parse JSON data into a slice of Entry structs
	// var entries []BirthChart
	// if err := json.Unmarshal(data, &entries); err != nil {
	// 	log.Fatalf("Failed to parse JSON: %v", err)
	// }

	log.Printf("Fetched %d entries\n", len(entries))

	var verifiedEntries []BirthChart
	var passed, failed int

	// Iterate through entries and validate with APIs
	for _, entry := range entries {

		// if err := DeleteBirthChartByID(db, entry.ID); err != nil {
		// 	log.Printf("Failed to delete entry %d: %v", entry.ID, err)
		// 	continue
		// }

		isValid := validateEntry(entry)

		if isValid {
			log.Printf("👑👑👑 Entry %d is valid\n", entry.ID)
			passed++
		} else {
			log.Printf("🥁🥁🥁 Entry %d is invalid\n", entry.ID)
			verifiedEntries = append(verifiedEntries, entry)
			failed++
		}
		// time.Sleep(time.Second) // Avoid hitting the API too frequently
	}

	// Save verified entries to JSON
	saveToJSON(verifiedEntries, fmt.Sprintf("lal_batch%d.json", batch))

	log.Printf("Summary: Tested %d entries - %d passed, %d failed.\n", len(entries), passed, failed)
}

// DeleteBirthChartByID deletes a row in the birth_chart_infos table based on the provided ID.
func DeleteBirthChartByID(db *sql.DB, id int) error {
	// Prepare the DELETE SQL statement
	stmt, err := db.Prepare("DELETE FROM birth_chart_infos WHERE id = $1;")
	if err != nil {
		return err
	}
	defer stmt.Close()

	// Execute the DELETE statement with the specified ID
	_, err = stmt.Exec(id)
	if err != nil {
		return err
	}

	return nil
}

func FetchRandomBirthCharts(db *sql.DB, offset, limit int) ([]BirthChart, error) {
	fmt.Println(limit, offset)
	// Attempt to query the database
	rows, err := db.Query("SELECT id, lat, lon, time, date FROM birth_chart_infos WHERE api_version = 'v2' ORDER BY id LIMIT $1 OFFSET $2;", limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer func() {
		// Attempt to close rows
		if closeErr := rows.Close(); closeErr != nil {
			fmt.Printf("warning: failed to close rows: %v\n", closeErr)
		}
	}()

	var entries []BirthChart
	for rows.Next() {
		var entry BirthChart
		// Attempt to scan each row
		if err := rows.Scan(&entry.ID, &entry.Lat, &entry.Lon, &entry.Time, &entry.Date); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		entries = append(entries, entry)
	}

	// Check for errors encountered during iteration
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error encountered during row iteration: %w", err)
	}

	return entries, nil
}

func saveToJSON(data []BirthChart, filename string) {
	file, _ := os.Create(filename)
	defer file.Close()
	jsonData, _ := json.MarshalIndent(data, "", "  ")
	file.Write(jsonData)
}

func validateEntry(entry BirthChart) bool {
	// Fetch data from both APIs (Generate API and Paid Verify API)
	generateData, err := FetchGenerateData(entry)
	if err != nil || generateData.Status != 200 {
		log.Println("Generate API error:", err)
		return false
	}

	// logrus.Info("generateData:", generateData)

	verifyData, err := FetchVerifyData(entry)
	if err != nil || verifyData.Status != 200 {
		log.Println("Verify API error:", err)
		return false
	}

	// Compare both data sets
	chartMatch := CompareCharts(generateData.Data, verifyData.Data.Data)

	return chartMatch
	// return false
}

func FetchGenerateData(entry BirthChart) (*GenerateResponse, error) {
	url := fmt.Sprintf("http://localhost:3000/v2/lal-kitab?type=birth&name=sanjay&date=%s&time=%s&lat=%s&lon=%s",
		entry.Date, entry.Time, entry.Lat, entry.Lon)

	log.Println("Generate API URL:", url)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data GenerateResponse
	body, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(body, &data)
	return &data, err
}

func FetchVerifyData(entry BirthChart) (*VerifyResponse, error) {
	day, month, year, hour, min := parseDateTime(entry.Date, entry.Time)
	url := fmt.Sprintf("http://localhost:3000/birth-details?day=%d&month=%d&year=%d&hour=%d&min=%d&lat=%s&lon=%s",
		day, month, year, hour, min, entry.Lat, entry.Lon)

	log.Println("Paid Verify API URL:", url)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var data VerifyResponse
	body, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(body, &data)
	return &data, err
}

func parseDateTime(date, timeStr string) (int, int, int, int, int) {
	parsedDate, _ := time.Parse("2006/01/02", date)
	parsedTime, _ := time.Parse("15:04", timeStr)
	return parsedDate.Day(), int(parsedDate.Month()), parsedDate.Year(), parsedTime.Hour(), parsedTime.Minute()
}

// Static structure to hold generated chart data
type GenerateData struct {
	D1Rashi   []int
	D1Planets []string
}

// Static structure to hold verified chart data
type VerifiedData struct {
	Sign        int
	SignName    string
	PlanetSmall []string
}

// SignMap maps rashi names to indices.
var SignMap = map[string]int{
	"Aries": 1, "Taurus": 2, "Gemini": 3, "Cancer": 4, "Leo": 5, "Virgo": 6,
	"Libra": 7, "Scorpio": 8, "Sagittarius": 9, "Capricorn": 10, "Aquarius": 11, "Pisces": 12,
}

// Static test data based on the logs
// var genData = GenerateData{
// 	D1Rashi:   []int{5, 6, 7, 8, 9, 10, 11, 12, 1, 2, 3, 4},
// 	D1Planets: []string{"As ₀₅", "", "", "Ma ₀₁ Ve ₀₈R Ke ₁₂", "Su ₂₅", "Me ₀₀c", "", "Mo ₁₅", "", "Sa ₂₉R Ra ₁₂", "", "Ju ↑ ₂₂R"},
// }

// {
// 	"place": "Houston, Texas",
// 	"country": "United States",
// 	"latitude": "29.787514",
// 	"longitude": "-95.710895"
// },
// {

// var verData = []VerifiedData{
// 	{"Sun", 5, "Sagittarius"},
// 	{"Moon", 8, "Pisces"},
// 	{"Mars", 4, "Scorpio"},
// 	{"Mercury", 6, "Capricorn"},
// 	{"Jupiter", 12, "Cancer"},
// 	{"Venus", 4, "Scorpio"},
// 	{"Saturn", 10, "Taurus"},
// 	{"Rahu", 10, "Taurus"},
// 	{"Ketu", 4, "Scorpio"},
// 	{"Ascendant", 1, "Leo"},
// }

// func main() {
// 	// Run compareCharts with static data
// 	result := CompareCharts(genData, verData)
// 	fmt.Println("Comparison result:", result)
// }

func CompareCharts(
	genData struct {
		D1Rashi   []int    `json:"lalRashi"`
		D1Planets []string `json:"lalPlanets"`
	},
	verData []struct {
		Sign        int      `json:"sign"`
		SignName    string   `json:"sign_name"`
		PlanetSmall []string `json:"planet_small"`
	}) bool {

	logrus.WithFields(logrus.Fields{
		"d1Rashi":   genData.D1Rashi,
		"d1Planets": genData.D1Planets,
		"verData":   verData,
	}).Info("Comparing charts")

	// Create a map for verified data by sign to make lookup easier
	verifyMap := make(map[int]struct {
		signIndex int
		planets   []string
	})

	// Fill verifyMap based on verified data (verData) structure
	for _, item := range verData {
		signIndex := item.Sign
		verifyMap[signIndex] = struct {
			signIndex int
			planets   []string
		}{
			signIndex: signIndex,
			planets:   item.PlanetSmall,
		}
	}

	// Iterate over each house and compare with generated data
	for house := range verifyMap {
		if genData.D1Rashi[house-1] != verifyMap[house].signIndex {
			logrus.Infof("Mismatch in Rashi for house %d: Expected %d, Found %d",
				house, verifyMap[house].signIndex, genData.D1Rashi[house-1])
			return false
		}

		// Extract planets from generated data using regex
		var planetRegex = regexp.MustCompile(`\b[A-Za-z]{2}\b`)
		genPlanets := planetRegex.FindAllString(genData.D1Planets[house-1], -1)

		logrus.Info("Generating planets for house ", house, ":", genPlanets)
		logrus.Info("verifying ", verifyMap[house].planets)
		logrus.Info("genPlanets ", len(genPlanets))
		logrus.Info("length ", len(verifyMap[house].planets))
		logrus.Info("check 1", house == 1)
		logrus.Info("check 2", len(genPlanets) != (len(verifyMap[house].planets)+1))
		logrus.Info("check 3", house == 1 && len(genPlanets) != (len(verifyMap[house].planets)+1))
		logrus.Info("check 4", len(genPlanets) != len(verifyMap[house].planets) || house == 1 && len(genPlanets) != (len(verifyMap[house].planets)+1))

		// Check if planets list lengths match
		if house != 1 && len(genPlanets) != len(verifyMap[house].planets) {
			logrus.Infof("Mismatch in planets count for house %d: Expected %d, Found %d",
				house, len(verifyMap[house].planets), len(genPlanets))
			return false
		}

		// Check if planets list lengths match
		if house == 1 && len(genPlanets) != (len(verifyMap[house].planets)+1) {
			logrus.Infof("Mismatch in planets count for house %d: Expected %d, Found %d",
				house, len(verifyMap[house].planets), len(genPlanets))
			return false
		}

		// Check if each planet in generated data matches the verified data for that house
		for _, genPlanet := range genPlanets {
			logrus.Info("genPlanet: ", genPlanet)
			logrus.Info("genPlanet: ", len(genPlanet))
			genPlanetName := genPlanet[:2]
			logrus.Info("genPlanetName: ", genPlanetName)
			logrus.Info("genPlanetName: ", len(genPlanetName))
			logrus.Info("verifyMap[house].planets : ", verifyMap[house].planets)

			found := false

			if genPlanetName == "As" {
				found = true
				break
			}

			for _, verPlanet := range verifyMap[house].planets {

				logrus.Info("verPlanet: ", verPlanet)
				logrus.Info("verPlanet: ", len(verPlanet))

				if genPlanetName == strings.Trim(verPlanet, " ") {
					found = true
					break
				}
			}
			if !found {
				logrus.Infof("Planet %s in house %d does not match verified data", genPlanetName, house)
				return false
			}
		}
	}

	return true
}

// var PlanetNameMap = map[string]string{
// 	"As": "Ascendant",
// 	"Ke": "Ketu",
// 	"Sa": "Saturn",
// 	"Ma": "Mars",
// 	"Ve": "Venus",
// 	"Su": "Sun",
// 	"Me": "Mercury",
// 	"Ra": "Rahu",
// 	"Mo": "Moon",
// 	"Ju": "Jupiter",
// }

// Helper to check if slice contains a string
// func contains(slice []string, item string) bool {
// 	for _, s := range slice {
// 		// logrus.Info("slice:", s, "item:", item, "equal:", s == item)
// 		if s == item {
// 			return true
// 		}
// 	}
// 	return false
// }
