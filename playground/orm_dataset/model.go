package main

import (
	"fmt"
	"strconv"
	"time"
)

// Car is a model mapped from vega-datasets cars.json documents.
type Car struct {
	ID uint64 `pirin:"pk,auto"`

	Name       string `pirin:"idx=name,string"`
	Origin     string `pirin:"idx=origin,string"`
	Cylinders  int    `pirin:"idx=cylinders,int"`
	Horsepower int    `pirin:"idx=horsepower,int"`
	Year       int    `pirin:"idx=year,int"`

	MPG          float64
	Displacement float64
	WeightInLbs  int
	Acceleration float64
}

type carDatasetDoc struct {
	Name           string   `json:"Name"`
	MilesPerGallon *float64 `json:"Miles_per_Gallon"`
	Cylinders      *float64 `json:"Cylinders"`
	Displacement   *float64 `json:"Displacement"`
	Horsepower     *float64 `json:"Horsepower"`
	WeightInLbs    *float64 `json:"Weight_in_lbs"`
	Acceleration   *float64 `json:"Acceleration"`
	Year           string   `json:"Year"`
	Origin         string   `json:"Origin"`
}

func (doc carDatasetDoc) toCar() *Car {
	return &Car{
		Name:         doc.Name,
		Origin:       doc.Origin,
		Cylinders:    toInt(doc.Cylinders),
		Horsepower:   toInt(doc.Horsepower),
		Year:         parseYear(doc.Year),
		MPG:          toFloat(doc.MilesPerGallon),
		Displacement: toFloat(doc.Displacement),
		WeightInLbs:  toInt(doc.WeightInLbs),
		Acceleration: toFloat(doc.Acceleration),
	}
}

func toInt(v *float64) int {
	if v == nil {
		return 0
	}
	return int(*v)
}

func toFloat(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func parseYear(raw string) int {
	if raw == "" {
		return 0
	}
	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02",
		"2006",
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, raw); err == nil {
			return ts.Year()
		}
	}
	if len(raw) >= 4 {
		if year, err := strconv.Atoi(raw[:4]); err == nil {
			return year
		}
	}
	return 0
}

func expandCar(base Car, replica int, row int) *Car {
	car := base
	car.Name = fmt.Sprintf("%s #%d", base.Name, row+1)

	if car.Year == 0 {
		car.Year = 1970 + (row % 20)
	} else {
		car.Year = car.Year + (replica % 4)
	}

	hpDelta := (row % 19) - 9
	car.Horsepower = clampInt(car.Horsepower+hpDelta, 45, 320)

	if car.Cylinders == 0 {
		car.Cylinders = 4 + (row % 5)
	}
	if car.WeightInLbs == 0 {
		car.WeightInLbs = 1500 + (row % 3000)
	}
	if car.MPG == 0 {
		car.MPG = 12 + float64(row%30)*0.5
	}

	return &car
}

func clampInt(v int, low int, high int) int {
	if v < low {
		return low
	}
	if v > high {
		return high
	}
	return v
}
