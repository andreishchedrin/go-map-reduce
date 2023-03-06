package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

const file = "Fifa22.csv"

type Player struct {
	Name        string
	Rating      uint8
	Position    string
	Nationality string
	Link        string
	Stats
}

type Stats struct {
	Pace      uint8
	Shoot     uint8
	Pass      uint8
	Dribbling uint8
	Defend    uint8
	Physics   uint8
}

func main() {
	start := time.Now()
	f, err := os.Open(file)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		panic(err)
	}

	lists := make(chan []Player)
	result := make(chan []Player)

	var wg sync.WaitGroup
	wg.Add(len(lines))

	for _, line := range lines {
		go func(player []string) {
			defer wg.Done()
			lists <- Map(player)
		}(line)
	}

	go Reducer(lists, result)

	wg.Wait()
	close(lists)

	final := <-result
	fmt.Println(time.Since(start))
	fmt.Println(final[1])
	fmt.Println(len(final))
}

func Map(player []string) []Player {
	list := make([]Player, 0)
	rating, _ := strconv.Atoi(player[1])
	pace, _ := strconv.Atoi(player[5])
	shoot, _ := strconv.Atoi(player[6])
	pass, _ := strconv.Atoi(player[7])
	dri, _ := strconv.Atoi(player[8])
	def, _ := strconv.Atoi(player[9])
	phy, _ := strconv.Atoi(player[10])

	list = append(list, Player{
		Name:        player[0],
		Rating:      uint8(rating),
		Position:    player[2],
		Nationality: player[4],
		Link:        player[11],
		Stats: Stats{
			Pace:      uint8(pace),
			Shoot:     uint8(shoot),
			Pass:      uint8(pass),
			Dribbling: uint8(dri),
			Defend:    uint8(def),
			Physics:   uint8(phy),
		},
	})

	return list
}

func Reducer(lists chan []Player, result chan []Player) {
	final := make([]Player, 0)
	for list := range lists {
		for _, value := range list {
			final = append(final, value)
		}
	}

	result <- final
}
