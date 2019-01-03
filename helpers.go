package main

import (
	"log"

	"github.com/alash3al/go-color"
)

func debug(msg string, v ...interface{}) {
	if !*flagVerbose {
		return
	}

	log.Println(color.YellowString(msg, v...))
}
