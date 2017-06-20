package main

import (
	"log"
	"os"
)

var debug = os.Getenv("FLOWERDEBUG") != ""
var dl = log.New(os.Stderr, "[flower-collectd] ", log.Lmicroseconds|log.Lmicroseconds)
