package vfsindex

import (
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// ConfigFile undocumented
type ConfigFile struct {
	Name2Index map[string]*ConfigIndex
}

//ConfigIndex undocumented
type ConfigIndex struct {
	Path string
	ConfigBase
}

// ConfigBase undocumented
type ConfigBase struct {
	Dir      string
	IndexDir string
	Table    string
}

// SaveCmdConfig undocumented
func SaveCmdConfig(dir string, conf *ConfigFile) error {

	f, err := os.OpenFile(filepath.Join(dir, "vfs-index.toml"), os.O_RDWR|os.O_CREATE, 0755)
	defer f.Close()
	if err != nil {
		return err
	}

	if err := toml.NewEncoder(f).Encode(conf); err != nil {
		return err
	}
	return nil
}

// LoadCmdConfig undocumented
func LoadCmdConfig(dir string) (*ConfigFile, error) {
	f, err := os.Open(filepath.Join(dir, "vfs-index.toml"))
	defer f.Close()

	if err != nil {
		return nil, err
	}
	conf := &ConfigFile{}

	_, err = toml.DecodeFile(filepath.Join(dir, "vfs-index.toml"), conf)
	if err != nil {
		return nil, err
	}

	return conf, nil
}
