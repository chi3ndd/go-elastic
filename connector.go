package elastic

import (
	"fmt"
	"os"
	"time"

	es7 "github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
	"github.com/x-cray/logrus-prefixed-formatter"
	es1 "gopkg.in/olivere/elastic.v2"
)

type (
	ConnectorV1 struct {
		Addr   string
		client *es1.Client
		logger *logrus.Logger
	}

	ConnectorV7 struct {
		Addr   string
		client *es7.Client
		logger *logrus.Logger
	}
)

func (con *ConnectorV1) Initiation() error {
	// Initiation logger
	con.logger = &logrus.Logger{
		Out:   os.Stderr,
		Level: logrus.DebugLevel,
		Formatter: &prefixed.TextFormatter{
			DisableColors:   false,
			TimestampFormat: time.RFC3339,
			FullTimestamp:   true,
			ForceFormatting: true,
		},
	}
	client, err := es1.NewSimpleClient(es1.SetURL(fmt.Sprintf("https://%s", con.Addr)))
	if err != nil {
		return err
	}
	con.client = client
	con.logger.Infof("Initializing connection to Elasticsearch [%s]", con.Addr)
	// Success
	return nil
}

func (con *ConnectorV7) Initiation() error {
	// Initiation logger
	con.logger = &logrus.Logger{
		Out:   os.Stderr,
		Level: logrus.DebugLevel,
		Formatter: &prefixed.TextFormatter{
			DisableColors:   false,
			TimestampFormat: time.RFC3339,
			FullTimestamp:   true,
			ForceFormatting: true,
		},
	}
	client, err := es7.NewSimpleClient(es7.SetURL(fmt.Sprintf("https://%s", con.Addr)))
	if err != nil {
		return err
	}
	con.client = client
	con.logger.Infof("Initializing connection to Elasticsearch [%s]", con.Addr)
	// Success
	return nil
}
