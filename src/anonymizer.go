package src

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/zeebo/blake3"
	"gopkg.in/yaml.v3"

	"github.com/Thearas/dodo/src/parser"
)

const (
	AnonymizeHashBytes = 8
)

var (
	miniDict     map[string]string
	miniLock     = sync.RWMutex{}
	miniReserves map[string]struct{}

	anonymizeMinLength       int
	anonymizerreserveIdHashs map[string]string

	// Identifiers that should not be anonymized.
	reserveIdentifiers = lo.Map([]string{
		"mysql",
		"global",
		"internal",
		"__internal_schema",
		"information_schema",
	}, func(s string, _ int) string { return strings.ToLower(s) })
)

func SetupAnonymizer(method, hashdictPath string, idMinLength int, reserveIds ...string) {
	reserveIdentifiers = append(reserveIdentifiers, reserveIds...)
	anonymizeMinLength = idMinLength
	anonymizerreserveIdHashs = anonymizeHashSliceToMap(reserveIdentifiers)

	if method == "minihash" {
		b, err := os.OpenFile(hashdictPath, os.O_RDONLY|os.O_CREATE, 0600)
		if err != nil {
			logrus.Fatalf("Failed to read hash dict file %s, err: %v\n", hashdictPath, err)
		}
		defer b.Close()

		miniDict = make(map[string]string)
		if err = yaml.NewDecoder(b).Decode(&miniDict); err != nil && err != io.EOF {
			logrus.Fatalf("Failed to decode hash dict file %s, err: %v\n", hashdictPath, err)
		}

		parser.DorisLexerInit()
		keywords := parser.DorisLexerLexerStaticData.SymbolicNames
		miniReserves = lo.SliceToMap(append(keywords, reserveIdentifiers...), func(s string) (string, struct{}) {
			return strings.ToLower(s), struct{}{}
		})
	}
}

func StoreMiniHashDict(method, hashdictPath string) {
	if method != "minihash" {
		return
	}

	newPath := hashdictPath + ".new"
	b, err := os.OpenFile(newPath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		logrus.Errorf("Failed to store hash dict file, err: %v\n", err)
		return
	}

	if err = yaml.NewEncoder(b).Encode(miniDict); err != nil {
		_ = b.Close()
		logrus.Errorf("Failed to encode hash dict file, err: %v\n", err)
		return
	}
	_ = b.Close()

	if err = os.Rename(newPath, hashdictPath); err != nil {
		logrus.Errorf("Failed to replace hash dict file, err: %v\n", err)
	}
}

func AnonymizeSql(method string, sqlId, sql string) string {
	anonymizeF := getAnonymizeFunc(method)
	if anonymizeF == nil {
		return sql
	}

	p := parser.NewParser(sqlId, sql, parser.NewListener(true, anonymizeF))
	s, err := p.ToSQL()
	if err != nil {
		// return original sql if fail to parse
		return sql
	}
	return s
}

func Anonymize(method string, s string) string {
	return getAnonymizeFunc(method)(s)
}

// NOTE: not thread safe.
func getAnonymizeFunc(method string) func(string) string {
	h := blake3.New()
	innerhashF := func(id string) (string, bool) {
		// FIXME: db/table name is case-insensitive
		lowerid := strings.ToLower(id)

		// only take the first AnonymizeHashBytes bytes of hash.
		hash := anonymizeHashStr(h, lowerid)

		// do not anoymize reserve ids.
		if _, ok := anonymizerreserveIdHashs[hash]; ok {
			return id, false
		}

		return hash, true
	}

	hashF := func(id string) string {
		// do not anoymize identifier that is less than MinAnonymizeLength characters.
		if len(id) < anonymizeMinLength {
			return id
		}

		hash, _ := innerhashF(id)
		return hash
	}

	minihashF := func(id string) string {
		hash, modified := innerhashF(id)
		if !modified {
			return id
		}
		return minifyHash(miniDict, hash)
	}

	switch method {
	case "hash":
		return hashF
	case "minihash":
		return minihashF
	default:
		logrus.Warnf("Anonymization method %s is not supported, keep going with no anonymization\n", method)
		return nil
	}
}

func anonymizeHashSliceToMap(xs []string) map[string]string {
	h := blake3.New()
	return lo.SliceToMap(xs, func(s string) (string, string) {
		return anonymizeHashStr(h, s), s
	})
}

func anonymizeHashStr(h *blake3.Hasher, s string) string {
	b := hashstr(h, s)

	return fmt.Sprintf("%x", b[:AnonymizeHashBytes])
}

func minifyHash(dict map[string]string, s string) string {
	miniLock.RLock()
	if mini, ok := dict[s]; ok {
		miniLock.RUnlock()
		return mini
	}

	miniLock.RUnlock()
	miniLock.Lock()
	defer miniLock.Unlock()

	lastWord, ok := dict["@@last"]
	if !ok || lastWord == "" {
		logrus.Debugln("Anonymization minify @@last word not found in hash dict, re-minifying all...")

		clear(dict)
		dict["@@last"] = "a"
		dict[s] = "a"
		return "a"
	}

	// gen next mini word: aa, ba, ca, ..., za, ab, ...
	for {
		var mini []rune
		for i, c := range lastWord {
			if c != 'z' {
				mini = append(mini, c+1)
				if i < len(lastWord)-1 {
					mini = append(mini, []rune(lastWord)[i+1:]...)
				}
				break
			}
			mini = append(mini, 'a')
			if i == len(lastWord)-1 {
				mini = append(mini, 'a')
			}
		}

		lastWord = string(mini)

		// prevent use keywords
		if _, ok := miniReserves[lastWord]; !ok {
			break
		}
	}

	dict["@@last"] = lastWord
	dict[s] = lastWord

	return lastWord
}
