package config

const (
	dbBadger = "badger"
)

func defaultConfig() *Config {
	return &Config{
		Proto: "dmqp",
		TCP: TCPConfig{
			IP:           "0.0.0.0",
			Port:         "9833",
			Nodelay:      false,
			ReadBufSize:  128 << 10, // 128Kb
			WriteBufSize: 128 << 10, // 128Kb
		},
		Queue: Queue{
			ShardSize:        8 << 10, // 8k
			MaxMessagesInRAM: 10 * 8 << 10, // 10 buckets
		},
		Db: Db{
			DefaultPath: "db",
			Engine:      dbBadger,
		},
		Manager: Manager{
			DefaultName: "/",
		},
		Connection: Connection{
			ChannelsMax:  4096,
			FrameMaxSize: 65536,
		},
	}
}
