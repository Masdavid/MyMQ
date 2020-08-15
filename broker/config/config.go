package config

import (
	"io/ioutil"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Proto      string
	TCP        TCPConfig
	Queue      Queue
	Db         Db
	Manager    Manager
	Connection Connection
}

type TCPConfig struct {
	IP           string `yaml:"ip"`
	Port         string
	Nodelay      bool
	ReadBufSize  int `yaml:"readBufSize"`
	WriteBufSize int `yaml:"writeBufSize"`
}

type Queue struct {
	ShardSize        int    `yaml:"shardSize"`
	MaxMessagesInRAM uint64 `yaml:"maxMessagesInRam"`
}

type Db struct {
	DefaultPath string `yaml:"defaultPath"`
	Engine      string `yaml:"engine"`
}

type Manager struct {
	DefaultName string `yaml:"defaultName"`
}

type Connection struct {
	ChannelsMax  uint16 `yaml:"channelsMax"`
	FrameMaxSize uint32 `yaml:"frameMaxSize"`
}

func CreateFromFile(path string) (*Config, error) {
	cfg := &Config{}
	file, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(file, &cfg)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func CreateDefault() (*Config, error) {
	return defaultConfig(), nil
}
