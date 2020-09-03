package config

import (
	"fmt"
	"os"
	"time"

	"github.com/RichardKnop/machinery/v1/log"
	"github.com/juju/fslock"
	"gopkg.in/fsnotify.v1"
	"gopkg.in/yaml.v2"
)

// NewFromYaml creates a config object from YAML file
func NewFromYaml(cnfPath string, keepReloading bool) (*Config, error) {
	cnf, err := fromFile(cnfPath)
	if err != nil {
		return nil, err
	}

	log.INFO.Printf("Successfully loaded config from file %s", cnfPath)

	if keepReloading {
		// creates a new file watcher
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.WARNING.Printf("Config Watcher ERROR", err)
		}
		// Open a goroutine to watch remote changes forever
		go func() {
			last := time.Now()
			lock := fslock.New(cnfPath)
			for {
				select {
				// watch for events
				case event := <-watcher.Events:
					if (event.Op.String() == "WRITE") && (time.Since(last).Seconds() > 5.0) {
						lock.Lock()
						// Attempt to reload the config
						newCnf, newErr := fromFile(cnfPath)
						if newErr == nil {
							*cnf = *newCnf
							log.INFO.Printf("Config reloaded from file %s", cnfPath)
							last = time.Now()
						}
						lock.Unlock()
					}
					// watch for errors
				case err := <-watcher.Errors:
					fmt.Println("ERROR", err)
				}
			}
		}()

		// out of the box fsnotify can watch a single file, or a single directory
		if err := watcher.Add(cnfPath); err != nil {
			log.WARNING.Printf("Config Watcher ERROR", err)
		}
	}

	return cnf, nil
}

// ReadFromFile reads data from a file
func ReadFromFile(cnfPath string) ([]byte, error) {
	file, err := os.Open(cnfPath)
	defer file.Close()

	// Config file not found
	if err != nil {
		return nil, fmt.Errorf("Open file error: %s", err)
	}

	// Config file found, let's try to read it
	data := make([]byte, 1000)
	count, err := file.Read(data)
	if err != nil {
		return nil, fmt.Errorf("Read from file error: %s", err)
	}

	return data[:count], nil
}

func fromFile(cnfPath string) (*Config, error) {
	loadedCnf, cnf := new(Config), new(Config)
	*cnf = *defaultCnf

	data, err := ReadFromFile(cnfPath)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(data, cnf); err != nil {
		return nil, fmt.Errorf("Unmarshal YAML error: %s", err)
	}
	if err := yaml.Unmarshal(data, loadedCnf); err != nil {
		return nil, fmt.Errorf("Unmarshal YAML error: %s", err)
	}
	if loadedCnf.AMQP == nil {
		cnf.AMQP = nil
	}

	return cnf, nil
}
