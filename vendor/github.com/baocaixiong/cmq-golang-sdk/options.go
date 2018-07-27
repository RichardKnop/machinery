package cmq

import (
	"strings"
	"net/http"
)

const (
	Topic = iota
	Queue
)

var (
	WideNetQueueUrl     = "https://cmq-queue-{region}.api.qcloud.com"
	WideNetTopicUrl     = "https://cmq-topic-{region}.api.qcloud.com"
	InternalNetQueueUrl = "http://cmq-queue-{region}.api.tencentyun.com"
	InternalNetTopicUrl = "http://cmq-topic-{region}.api.tencentyun.com"
)

type Options struct {
	Region     string            `yaml:"region"`
	Credential *Credential       `yaml:"credential"`
	NetEnv     string            `yaml:"net_env"`
	queueUrl   string            `yaml:"-"`
	topicUrl   string            `yaml:"-"`
	transport  http.RoundTripper `yaml:"-"`
}

type Credential struct {
	SecretId  string `yaml:"secret_id"`
	SecretKey string `yaml:"secret_key"`
}

func newOptions(opts ...Option) *Options {
	options := new(Options)

	for k := range opts {
		opts[k](options)
	}

	if len(options.Region) < 1 {
		panic("region can not empty")
	}

	if options.NetEnv == "lan" {
		options.queueUrl = strings.Replace(InternalNetQueueUrl, "{region}", options.Region, 1)
		options.topicUrl = strings.Replace(InternalNetTopicUrl, "{region}", options.Region, 1)
	} else if options.NetEnv == "wan" {
		options.queueUrl = strings.Replace(WideNetQueueUrl, "{region}", options.Region, 1)
		options.topicUrl = strings.Replace(WideNetTopicUrl, "{region}", options.Region, 1)
	} else {
		panic("net env must be [wan lan]")
	}

	return options
}

func Region(r string) Option {
	return func(opts *Options) {
		opts.Region = r
	}
}

func NetEnv(e string) Option {
	return func(o *Options) {
		if o.Region == "" {
			panic("net env can not empty")
		}
		o.NetEnv = e
	}
}

func SetCredential(c *Credential) Option {
	return func(opts *Options) {
		opts.Credential = c
	}
}

func Transaction(t *http.Transport) Option {
	return func(opts *Options) {
		opts.transport = t
	}
}
