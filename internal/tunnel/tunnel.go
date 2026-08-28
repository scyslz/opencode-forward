package tunnel

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Record struct {
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

type Store struct {
	mu      sync.Mutex
	Path    string
	records map[string]*Record
}

func NewStore(path string) *Store {
	s := &Store{Path: path, records: map[string]*Record{}}
	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			var disk map[string]*Record
			if json.Unmarshal(data, &disk) == nil && disk != nil {
				s.records = disk
			}
		}
	}
	return s
}

func (s *Store) save() {
	if s.Path == "" {
		return
	}
	data, err := json.MarshalIndent(s.records, "", "  ")
	if err != nil {
		return
	}
	dir := filepath.Dir(s.Path)
	if dir != "" {
		_ = os.MkdirAll(dir, 0o755)
	}
	tmp := s.Path + ".tmp"
	if os.WriteFile(tmp, data, 0o644) == nil {
		_ = os.Rename(tmp, s.Path)
	}
}

func (s *Store) Open(id, dir, peerID, remote, local string) *Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := &Record{
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

func (s *Store) Heartbeat(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.records[id]; r != nil {
		r.LastFrame = time.Now()
		r.Frames++
		s.save()
	}
}
func (s *Store) Close(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := s.records[id]; r != nil {
		r.ClosedAt = time.Now()
		r.Status = "down"
		s.save()
	}
}
