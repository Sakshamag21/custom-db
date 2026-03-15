package coreDB

import (
	"encoding/binary"
	"encoding/json"
	"hash/fnv"
	"os"
	// "path/filepath"
)

type BloomFilter struct {
	Size uint
	K    uint
	Bits []byte
}

type BloomConfig struct {
	Columns []string
	Size    uint
	Hashes  uint
}

type BloomFile struct {
	Columns map[string]*BloomFilter `json:"columns"`
}

func NewBloomFilter(size uint, k uint) *BloomFilter {
	byteSize := (size + 7) / 8

	return &BloomFilter{
		Size: size,
		K:    k,
		Bits: make([]byte, byteSize),
	}
}

func (bf *BloomFilter) hash(data []byte, seed uint32) uint {
	h := fnv.New32a()

	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], seed)

	h.Write(b[:])
	h.Write(data)

	return uint(h.Sum32()) % bf.Size
}

func (bf *BloomFilter) Add(value string) {
	data := []byte(value)

	for i := uint(0); i < bf.K; i++ {
		pos := bf.hash(data, uint32(i))

		byteIndex := pos / 8
		bitIndex := pos % 8

		bf.Bits[byteIndex] |= 1 << bitIndex
	}
}

func (bf *BloomFilter) MightContain(value string) bool {
	data := []byte(value)

	for i := uint(0); i < bf.K; i++ {
		pos := bf.hash(data, uint32(i))

		byteIndex := pos / 8
		bitIndex := pos % 8

		if bf.Bits[byteIndex]&(1<<bitIndex) == 0 {
			return false
		}

	}

	return true
}

func toString(v interface{}) string {

	switch t := v.(type) {

	case string:
		return t

	case int32:
		return string(rune(t))

	case int64:
		return string(rune(t))

	default:
		return ""
	}
}

func buildBloomFilters(records []Record, config BloomConfig) map[string]*BloomFilter {
	result := make(map[string]*BloomFilter)
	for _, col := range config.Columns {

		bf := NewBloomFilter(config.Size, config.Hashes)

		for _, r := range records {

			val, ok := r[col]

			if !ok {
				continue
			}

			str := toString(val)

			bf.Add(str)
		}

		result[col] = bf
	}

	return result
}

func saveBloomFile(path string, blooms map[string]*BloomFilter) error {

	bf := BloomFile{
		Columns: blooms,
	}

	data, err := json.Marshal(bf)
	if err != nil {
		return err
	}

	bloomPath := path + ".bloom"

	return os.WriteFile(bloomPath, data, 0644)
}

func loadBloomFile(path string) (BloomFile, error) {

	var bf BloomFile

	data, err := os.ReadFile(path + ".bloom")

	if err != nil {
		return bf, err
	}

	err = json.Unmarshal(data, &bf)

	return bf, err
}
