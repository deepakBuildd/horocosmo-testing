package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
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
		KpRashi   []int    `json:"kpRashi"`
		KpPlanets []string `json:"kpPlanets"`
	} `json:"data"`
}

type Data struct {
	Signs        []int    `json:"signs"`
	Planets      []string `json:"planets"`
	PlanetsSmall []string `json:"planets_small"`
	PlanetSigns  []int    `json:"planet_signs"`
}

type VerifyResponse struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
	Data    struct {
		Data []Data `json:"data"`
	} `json:"data"`
}

func main() {
	// Connect to database
	db, err := sql.Open("postgres", "postgresql://horocosmo:horocosmo@3.111.219.25:5432/horocosmo?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// batchSize := 1 // Define the batch size
	// // Track batch number
	batch := 0

	// Fetch random entries
	// entries, err := FetchRandomBirthCharts(db, batch*batchSize, batchSize)
	// if err != nil {
	// 	log.Fatalf("Failed to fetch random birth charts: %v", err)
	// }

	// Read JSON file
	data, err := os.ReadFile("batch4.json") // replace "data.json" with the path to your JSON file
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}

	// Parse JSON data into a slice of Entry structs
	var entries []BirthChart
	if err := json.Unmarshal(data, &entries); err != nil {
		log.Fatalf("Failed to parse JSON: %v", err)
	}

	log.Printf("Fetched %d entries\n", len(entries))

	var verifiedEntries []BirthChart
	var passed, failed int

	// Iterate through entries and validate with APIs
	for _, entry := range entries {

		// deletedRows, err := DeleteBirthChartByEntry(db, entry)
		// if err != nil {
		// 	log.Fatalf("Failed to delete birth_chart_infos entry: %v", err)
		// } else {
		// 	log.Printf("Successfully deleted %d birth_chart_infos entry(ies)", deletedRows)
		// }

		isValid := ValidateEntry(entry)

		if isValid {
			log.Printf("👑👑👑 Entry %d is valid\n", entry.ID)
			passed++
		} else {
			log.Printf("🥁🥁🥁 Entry %d is invalid\n", entry.ID)
			verifiedEntries = append(verifiedEntries, entry)
			failed++
		}
		time.Sleep(time.Second) // Avoid hitting the API too frequently
	}

	// Save verified entries to JSON
	saveToJSON(verifiedEntries, fmt.Sprintf("kp_batch%d.json", batch))

	log.Printf("Summary: Tested %d entries - %d passed, %d failed.\n", len(entries), passed, failed)
}

// DeleteUserByEntry deletes a row from the users table based on matching the latitude, longitude, time, date_of_birth, and location fields.
func DeleteUserByEntry(db *sql.DB, entry BirthChart) (int64, error) {
	// Prepare the DELETE SQL statement with placeholders for values
	stmt, err := db.Prepare(`DELETE FROM users 
		WHERE latitude = $1 
		AND longitude = $2 
		AND time = $3 
		AND date_of_birth = $4;`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// Execute the DELETE statement with the specified field values
	res, err := stmt.Exec(entry.Lat, entry.Lon, entry.Time, entry.Date)
	if err != nil {
		return 0, err
	}

	// Get the number of rows affected
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

// DeleteBirthChartByEntry deletes a row from the birth_chart_infos table based on matching lat, lon, date, and time fields.
func DeleteBirthChartByEntry(db *sql.DB, entry BirthChart) (int64, error) {
	// Begin a transaction
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}

	// Find all birth_chart_info IDs that match the entry criteria
	rows, err := tx.Query(`SELECT id FROM birth_chart_infos WHERE lat = $1 AND lon = $2 AND date = $3 AND time = $4`, entry.Lat, entry.Lon, entry.Date, entry.Time)
	if err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("failed to retrieve birth_chart_info ids: %v", err)
	}
	defer rows.Close()

	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("failed to scan id: %v", err)
		}
		ids = append(ids, id)
	}

	if err := rows.Err(); err != nil {
		tx.Rollback()
		return 0, fmt.Errorf("error iterating rows: %v", err)
	}

	// Track rows deleted for the birth chart itself
	var totalRowsDeleted int64

	// Delete associated records for each found ID
	for _, id := range ids {
		// Delete from prediction_infos
		if _, err := tx.Exec(`DELETE FROM prediction_infos WHERE birth_chart_info_id = $1`, id); err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("failed to delete from prediction_infos: %v", err)
		}

		// Delete from sign_data
		if _, err := tx.Exec(`DELETE FROM sign_data WHERE birth_chart_info_id = $1`, id); err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("failed to delete from sign_data: %v", err)
		}

		// Delete from lk_prediction_infos
		if _, err := tx.Exec(`DELETE FROM lk_prediction_infos WHERE birth_chart_info_id = $1`, id); err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("failed to delete from lk_prediction_infos: %v", err)
		}

		// Delete the main birth_chart_info entry
		res, err := tx.Exec(`DELETE FROM birth_chart_infos WHERE id = $1`, id)
		if err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("failed to delete birth_chart_info entry: %v", err)
		}

		// Count the number of rows deleted for birth_chart_infos
		rowsDeleted, err := res.RowsAffected()
		if err != nil {
			tx.Rollback()
			return 0, fmt.Errorf("failed to retrieve rows affected: %v", err)
		}
		totalRowsDeleted += rowsDeleted
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %v", err)
	}

	return totalRowsDeleted, nil
}

// DeleteBirthChartByID deletes a row in the birth_chart_infos table based on the provided ID.
func DeleteBirthChartByID(db *sql.DB, id int) (int64, error) {
	// Prepare the DELETE SQL statement
	stmt, err := db.Prepare("DELETE FROM birth_chart_infos WHERE id = $1;")
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	// Execute the DELETE statement with the specified ID
	res, err := stmt.Exec(id)
	if err != nil {
		return 0, err
	}
	// Get the number of rows affected
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
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

func ValidateEntry(entry BirthChart) bool {
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
	url := fmt.Sprintf("http://localhost:3000/v2/kp-chart?type=birth&name=sanjay&date=%s&time=%s&lat=%s&lon=%s",
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
	url := fmt.Sprintf("http://localhost:3000/astro-api?day=%d&month=%d&year=%d&hour=%d&min=%d&lat=%s&lon=%s&api=kp",
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

// SignMap maps rashi names to indices.
var SignMap = map[string]int{
	"Aries": 1, "Taurus": 2, "Gemini": 3, "Cancer": 4, "Leo": 5, "Virgo": 6,
	"Libra": 7, "Scorpio": 8, "Sagittarius": 9, "Capricorn": 10, "Aquarius": 11, "Pisces": 12,
}

func CompareCharts(
	genData struct {
		KpRashi   []int    `json:"kpRashi"`
		KpPlanets []string `json:"kpPlanets"`
	},
	verData []Data) bool {

	logrus.WithFields(logrus.Fields{
		"kpRashi":   genData.KpRashi,
		"kpPlanets": genData.KpPlanets,
		"verData":   verData,
	}).Info("Comparing charts")

	// Validate input data lengths
	if len(genData.KpRashi) != len(genData.KpPlanets) {
		logrus.Error("Mismatch in genData lengths")
		return false
	}

	// Iterate through generated data
	for i, planetShortNames := range genData.KpPlanets {
		// Skip empty planet names
		if planetShortNames == "" {
			continue
		}

		// Split multiple planet short names
		splitPlanets := strings.Fields(planetShortNames)

		// Current house is i+1 (as houses are 1-indexed)
		currentHouse := i + 1
		currentRashi := genData.KpRashi[i]

		// Verify rashi matches first sign in the entry's signs array
		if verData[i].Signs[0] != currentRashi {
			logrus.Errorf("Rashi mismatch for house %d: expected %d, got %d",
				currentHouse, currentRashi, verData[i].Signs[0])
			return false
		}

		// Check each planet in the current house
		for _, planetShortName := range splitPlanets {
			// Get full planet name
			fullPlanetName, ok := PlanetNameMap[planetShortName]
			if !ok {
				logrus.Errorf("Unknown planet short name: %s", planetShortName)
				return false
			}

			if fullPlanetName == "Ascendant" {
				continue
			}

			// Check if planet exists in verification data
			planetFound := false
			for j, smallPlanet := range verData[i].PlanetsSmall {
				logrus.WithFields(logrus.Fields{
					"smallPlanet":     smallPlanet,
					"planetShortName": planetShortName,
					"planetMatch":     strings.TrimSpace(smallPlanet) == planetShortName,
				}).Info("Checking planet ...")
				if strings.TrimSpace(smallPlanet) == planetShortName {
					// Verify planet name matches
					if len(verData[i].Planets) > j &&
						verData[i].Planets[j] != fullPlanetName {
						logrus.Errorf("Planet name mismatch: short %s, full %s",
							planetShortName, fullPlanetName)
						return false
					}
					planetFound = true
					break
				}
			}

			if !planetFound {
				logrus.Errorf("Planet %s not found in house %d",
					fullPlanetName, currentHouse)
				return false
			}
		}
	}

	return true
}

var PlanetNameMap = map[string]string{
	"As": "Ascendant",
	"Ke": "Ketu",
	"Sa": "Saturn",
	"Ma": "Mars",
	"Ve": "Venus",
	"Su": "Sun",
	"Me": "Mercury",
	"Ra": "Rahu",
	"Mo": "Moon",
	"Ju": "Jupiter",
}

// // Helper to check if slice contains a string
// func contains(slice []string, item string) bool {
// 	for _, s := range slice {
// 		// logrus.Info("slice:", s, "item:", item, "equal:", s == item)
// 		if s == item {
// 			return true
// 		}
// 	}
// 	return false
// }
