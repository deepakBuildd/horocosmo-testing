package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

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
	Data    Data   `json:"data"`
}

type Data struct {
	Birth struct {
		Planets []string `json:"planets"`
		Rashi   []int    `json:"rashi"`
	} `json:"birth"`
}

type VerifyResponse struct {
	HousePlanets map[string][]string `json:"housePlanets"`
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
	data, err := os.ReadFile("batch1.json") // replace "data.json" with the path to your JSON file
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
	saveToJSON(verifiedEntries, fmt.Sprintf("transit_batch%d.json", batch))

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
	if err != nil {
		log.Println("Verify API error:", err)
		return false
	}

	// Compare both data sets
	chartMatch := CompareCharts(generateData.Data, verifyData)

	return chartMatch
	// return false
}

func FetchGenerateData(entry BirthChart) (*GenerateResponse, error) {
	url := fmt.Sprintf("http://localhost:3000/v2/transit-chart?type=birth&name=sanjay&date=%s&time=%s&lat=%s&lon=%s",
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
	url := fmt.Sprintf("http://localhost:4000/scraper?day=%d&month=%d&year=%d&hour=%d&min=%d&lat=%s&lon=%s",
		day, month, year, hour, min, entry.Lat, entry.Lon)

	log.Println("Paid Verify API URL:", url)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	logrus.Info("resp body:", resp.Body)

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
	genData Data,
	verData *VerifyResponse) bool {

	logrus.WithFields(logrus.Fields{
		"genData": genData,
		"verData": verData,
	}).Info("Comparing charts")

	// Helper function to clean planet names from subscripts and special chars
	cleanPlanetName := func(name string) string {
		// Split at first non-letter character
		cleanName := ""
		for _, r := range name {
			if !unicode.IsLetter(r) {
				break
			}
			cleanName += string(r)
		}

		logrus.WithFields(logrus.Fields{
			"cleanName": cleanName,
			"name":      name,
		}).Info("cleanPlanetName parsing...")

		return cleanName
	}

	// Helper function to extract all planet names from a string
	extractPlanets := func(planetStr string) []string {
		var planets []string
		parts := strings.Fields(planetStr)

		// Process each part
		for i := 0; i < len(parts); i++ {
			cleanName := cleanPlanetName(parts[i])
			if cleanName != "" {
				planets = append(planets, cleanName)
			}
		}

		return planets
	}

	// Create a map of house -> planets from genData
	genHousePlanets := make(map[string][]string)

	for i, planetStr := range genData.Birth.Planets {
		if planetStr == "" {
			continue
		}

		house := strconv.Itoa(i + 1)
		planets := extractPlanets(planetStr)

		for _, planet := range planets {
			// Convert short name to full name
			fullName, ok := PlanetNameMap[planet]
			if !ok {
				logrus.Errorf("Unknown planet short name: %s in %s", planet, planetStr)
				return false
			}

			genHousePlanets[house] = append(genHousePlanets[house], fullName)
		}
	}

	logrus.WithFields(logrus.Fields{
		"genHousePlanets": genHousePlanets,
		"verHousePlanets": verData.HousePlanets,
	}).Info("Processed planet maps")

	// Compare the two maps
	for house, verPlanets := range verData.HousePlanets {
		genPlanets, exists := genHousePlanets[house]
		if !exists {
			// If house exists in verData but not in genData
			if len(verPlanets) > 0 {
				logrus.Errorf("House %s missing in generated data but has planets in verification data", house)
				return false
			}
			continue
		}

		// Sort both slices for comparison
		sort.Strings(verPlanets)
		sort.Strings(genPlanets)

		// Compare planet lists
		if !reflect.DeepEqual(verPlanets, genPlanets) {
			logrus.Errorf("Planet mismatch in house %s: expected %v, got %v",
				house, verPlanets, genPlanets)
			return false
		}
	}

	// Also check if genData has any houses with planets that verData doesn't have
	for house, genPlanets := range genHousePlanets {
		if _, exists := verData.HousePlanets[house]; !exists && len(genPlanets) > 0 {
			logrus.Errorf("House %s exists in generated data but missing in verification data", house)
			return false
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
