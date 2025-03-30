package main

import (
	"fmt"
	"github.com/mattn/go-colorable"
	json "github.com/neilotoole/jsoncolor"
	"net/http"
	"os"
)

func BuildURL(settings *Settings, endpoint string) string {
	protocol := "http"
	if settings.UseHTTPS {
		protocol = "https"
	}
	return fmt.Sprintf("%s://%s:%d%s", protocol, settings.Host, settings.Port, endpoint)
}

func PrintJSONResponse(resp *http.Response) {
	var data any
	var enc *json.Encoder

	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		fmt.Println("Failed to parse response:", err)
		return
	}
	if json.IsColorTerminal(os.Stdout) {
		out := colorable.NewColorable(os.Stdout) // needed for Windows
		enc = json.NewEncoder(out)
		clrs := json.DefaultColors()
		enc.SetColors(clrs)
		enc.SetIndent("", "  ")
	} else {
		enc = json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(data); err != nil {
		fmt.Println("Failed to encode response:", err)
	}
}
