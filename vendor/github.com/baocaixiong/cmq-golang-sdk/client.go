package cmq

import (
	"net/http"
	"net/url"
	"github.com/baocaixiong/cmq-golang-sdk/models"
)

var UriSec = "/v2/index.php"

type Client struct {
	httpClient *http.Client
	opt        *Options
}

type Option func(*Options)

func NewClient(opts ...Option) *Client {
	options := newOptions(opts...)

	return &Client{
		opt:        options,
		httpClient: &http.Client{},
	}
}

func (c *Client) Init(opts ...Option) {
	for k := range opts {
		opts[k](c.opt)
	}
}

func (c *Client) Send(request models.IRequest, response models.IResponse) (err error) {
	if err := models.ContactParams(request); err != nil {
		return err
	}
	request.GetParams()["SecretId"] = c.opt.Credential.SecretId
	models.CompleteCommonParams(request, c.opt.Region)

	var uri string
	if request.GetType() == "topic" {
		uri = c.opt.topicUrl
	} else {
		uri = c.opt.queueUrl
	}
	uri += UriSec
	u, err := url.Parse(uri)
	if err != nil {
		return
	}
	signRequest(request, u.Host, c.opt.Credential, SHA1)

	httpRequest, err := http.NewRequest(request.GetHttpMethod(), uri, request.GetBodyReader())
	if err != nil {
		return
	}
	//trace := &httptrace.ClientTrace{
	//	DNSDone: func(dnsInfo httptrace.DNSDoneInfo) {
	//		log.Printf("dns done info: %+v\n", dnsInfo)
	//	},
	//	GotConn: func(connInfo httptrace.GotConnInfo) {
	//		log.Printf("Got Conn: %+v\n", connInfo)
	//	},
	//}
	//httpRequest = httpRequest.WithContext(httptrace.WithClientTrace(httpRequest.Context(), trace))
	httpRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	httpResponse, err := c.httpClient.Do(httpRequest)
	if err != nil {
		return err
	}
	err = models.ParseFromHttpResponse(httpResponse, response)
	return
}
