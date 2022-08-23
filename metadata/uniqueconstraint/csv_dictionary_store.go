package uniqueconstraint

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
)

//FileReader file reader interface
type FileReader interface {
	ReadFile(filePath string) ([]byte, error)
}

type defaultFileReader struct {
}

func (d *defaultFileReader) ReadFile(filePath string) ([]byte, error) {
	return ioutil.ReadFile(filePath)
}

//CSVDictionaryStore local csv as source of unique constraint
//CSV files separated by semicolon(;) rather than comma(,)
//with or without header, header is not required
// project.dataset.tablename;id,status
type CSVDictionaryStore struct {
	FilePath   string
	FileReader FileReader
}

//NewCSVDictionaryStore is constructor of CSVUniqueConstraintDictionaryStore
func NewCSVDictionaryStore(filePath string, fileReader FileReader) *CSVDictionaryStore {
	return &CSVDictionaryStore{
		FilePath:   filePath,
		FileReader: fileReader,
	}
}

//Get to read csv file and parse into unique constraint dictionary
func (l *CSVDictionaryStore) Get() (map[string][]string, error) {
	msg := fmt.Sprintf("readic csv from %s", l.FilePath)
	logger.Println(msg)

	content, err := l.FileReader.ReadFile(l.FilePath)
	if err != nil {
		return nil, err
	}

	dict, err := parse(string(content))
	if err != nil {
		return nil, err
	}

	msg = fmt.Sprintf("get dictionary from csv file, found : %d tables", len(dict))
	logger.Println(msg)

	return dict, nil
}

func parse(content string) (map[string][]string, error) {
	uniqueConstrainsDict := make(map[string][]string)

	lines := strings.Split(content, "\n")
	for _, line := range lines {
		tableID, uniqueConstraints, err := parseLine(line)
		if err != nil {
			return nil, err
		}
		uniqueConstrainsDict[tableID] = uniqueConstraints
	}

	return uniqueConstrainsDict, nil
}

func parseLine(line string) (string, []string, error) {
	row := strings.Split(line, ";")

	if len(row) != 2 {
		return "", nil, errors.New("csv format error")
	}

	ID := row[0]
	constraints := strings.Split(row[1], ",")
	return ID, constraints, nil
}
