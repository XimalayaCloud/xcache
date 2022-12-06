// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package fe

import (
	"bytes"

	"github.com/BurntSushi/toml"

	//"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	//"github.com/CodisLabs/codis/pkg/utils/timesize"
)

const DefaultConfig = `
##################################################
#                                                #
#                  Codis-Fe                      #
#                                                #
##################################################
# Startup config.
# Set the listen address.
listen = ""

# Set runtime.GOMAXPROCS to N, default is 1.
ncpu = 1

# Set path/name of daliy rotated log file.
log = "fe.log"

# Set pidfile
pidfile = "fe.pid"

# Set the log-level, should be INFO,WARN,DEBUG or ERROR, default is INFO.
log_level = "info"

# Expire-log-days, 0 means never expire
expire_log_days = 30

# Set list of dashboard.
dashboard_list = ""

# Set mysql server (such as localhost:3306), fe will report user info and codis info to mysql.
mysql_addr = ""
mysql_username = ""
mysql_password = ""
mysql_database = ""
`

type Config struct {
	MysqlAddr     string  `toml:"mysql_addr"`
	MysqlUsername string  `toml:"mysql_username"`
	MysqlPassword string  `toml:"mysql_password" json:"-"`
	MysqlDatabase string  `toml:"mysql_database"`
	//TablePrefix   string  `toml:"table_prefix"`
	Ncpu          int     `toml:"ncpu"`
	Log           string  `toml:"log"`
	ExpireLogDays int     `toml:"expire_log_days"`
	LogLevel      string  `toml:"log_level"`
	DashboardList string  `toml:"dashboard_list"`
	Listen        string  `toml:"listen"`
	PidFile       string  `toml:"pidfile"`
}

func NewDefaultConfig() *Config {
	c := &Config{}
	if _, err := toml.Decode(DefaultConfig, c); err != nil {
		log.PanicErrorf(err, "decode toml failed")
	}
	/*if err := c.Validate(); err != nil {
		log.PanicErrorf(err, "validate config failed")
	}*/
	return c
}

func (c *Config) LoadFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	if err != nil {
		return errors.Trace(err)
	}
	return c.Validate()
}

func (c *Config) String() string {
	var b bytes.Buffer
	e := toml.NewEncoder(&b)
	e.Indent = "    "
	e.Encode(c)
	return b.String()
}

func (c *Config) Validate() error {
	if c.MysqlAddr == "" {
		return errors.New("invalid mysql_addr")
	}
	if c.MysqlUsername == "" {
		return errors.New("invalid mysql_username")
	}
	if c.MysqlPassword == "" {
		return errors.New("invalid mysql_password")
	}
	if c.MysqlDatabase == "" {
		return errors.New("invalid mysql_database")
	}
	if c.Ncpu <= 0 {
		return errors.New("invalid ncpu")
	}
	if c.Log == "" {
		return errors.New("invalid log")
	}
	if c.LogLevel == "" {
		return errors.New("invalid log level")
	}
	if c.ExpireLogDays < 0 {
		return errors.New("invalid expire_log_days")
	}
	return nil
}
