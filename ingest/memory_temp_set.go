package ingest

// memoryTempSet is an in-memory implementation of TempSet interface.
// As of July 2019 this requires up to ~4GB of memory for pubnet ledger
// state processing. The internal structure is dereferenced after the
// store is closed.
type memoryTempSet struct {
	m map[string]struct{}
}

// Open initialize internals data structure.
func (s *memoryTempSet) Open() error {
	s.m = make(map[string]struct{})
	return nil
}

// Add adds a key to TempSet.
func (s *memoryTempSet) Add(key string) error {
	s.m[key] = struct{}{}
	return nil
}

// Exist check if the key exists in a TempSet.
func (s *memoryTempSet) Exist(key string) (bool, error) {
	_, exists := s.m[key]
	return exists, nil
}

// Remove removes a key from the TempSet
func (s *memoryTempSet) Remove(key string) error {
	delete(s.m, key)
	return nil
}

// Close removes reference to internal data structure.
func (s *memoryTempSet) Close() error {
	s.m = nil
	return nil
}
