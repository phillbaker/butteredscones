package butteredscones

import (
	"encoding/json"
	"net/http"
)

// StatisticsServer constructs an HTTP server that returns JSON formatted
// statistics. These statistics can be used for debugging or automated
// monitoring.
type StatisticsServer struct {
	Statistics *Statistics
	Addr       string
}

func (s *StatisticsServer) ListenAndServe() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)

	server := &http.Server{
		Addr:    s.Addr,
		Handler: mux,
	}

	return server.ListenAndServe()
}

func (s *StatisticsServer) handleRoot(writer http.ResponseWriter, request *http.Request) {
	s.Statistics.UpdateFileSizeStatistics()

	jsonStats, err := json.Marshal(s.Statistics)
	if err != nil {
		writer.WriteHeader(500)
		writer.Write([]byte(err.Error()))
	} else {
		writer.Header().Add("Content-Type", "application/json")
		writer.Write(jsonStats)
	}
}
