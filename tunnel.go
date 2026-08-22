package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type tunnelRecord struct {
	ID        string    `json:"id"`
	Dir       string    `json:"dir"`
	PeerID    string    `json:"peer_id"`
	Remote    string    `json:"remote"`
	Local     string    `json:"local"`
	Connected time.Time `json:"connected"`
	ClosedAt  time.Time `json:"closed_at"`
	LastFrame time.Time `json:"last_frame"`
	Status    string    `json:"status"`
	Frames    int64     `json:"frames"`
}

type tunnelStore struct {
	mu      sync.Mutex
	path    string
	records map[string]*tunnelRecord
}

func newTunnelStore(path string) *tunnelStore {
	s := &tunnelStore{path: path, records: map[string]*tunnelRecord{}}
	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			var disk map[string]*tunnelRecord
			if json.Unmarshal(data, &disk) == nil && disk != nil {
				s.records = disk
			}
		}
	}
	return s
}

func (s *tunnelStore) save() {
	if s.path == "" {
		return
	}
	data, err := json.MarshalIndent(s.records, "", "  ")
	if err != nil {
		return
	}
	dir := filepath.Dir(s.path)
	if dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	tmp := s.path + ".tmp"
	if os.WriteFile(tmp, data, 0o644) == nil {
		_ = os.Rename(tmp, s.path)
	}
}

func (s *tunnelStore) open(id, dir, peerID, remote, local string) *tunnelRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := &tunnelRecord{
		ID:        id,
		Dir:       dir,
		PeerID:    peerID,
		Remote:    remote,
		Local:     local,
		Connected: time.Now(),
		Status:    "up",
	}
	s.records[id] = r
	s.save()
	return r
}

func (s *tunnelStore) touch(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.records[id]; r != nil {
		r.LastFrame = time.Now()
	}
}
func (s *tunnelStore) frame(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.records[id]; r != nil {
		r.LastFrame = time.Now()
		r.Frames++
	}
}
func (s *tunnelStore) heartbeat(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.records[id]; r != nil {
		r.LastFrame = time.Now()
		r.Frames++
		s.save()
	}
}
func (s *tunnelStore) close(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.records[id]; r != nil {
		r.ClosedAt = time.Now()
		r.Status = "down"
		s.save()
	}
}
