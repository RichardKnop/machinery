package models

import (
	"encoding/json"
	"io"
	"math/rand"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type IRequest interface {
	GetParams() map[string]string
	GetAction() string
	GetBodyReader() io.Reader
	GetType() string
	GetHttpMethod() string
}

func init() {
	rand.Seed(time.Now().Unix())
}

type Request struct {
	Action          string `name:"Action"`
	Region          string `name:"Region"`
	Timestamp       uint64 `name:"Timestamp"`
	Nonce           uint64 `name:"Nonce"`
	SecretId        string `name:"SecretId"`
	Signature       string `name:"Signature"`
	SignatureMethod string `name:"SignatureMethod"`
	Token           string `name:"Token"`

	params map[string]string
	domain string
	t      string
}

func InitReq(a string, t string) *Request {
	return &Request{
		params: make(map[string]string),
		Action: a,
		t:      t,
	}
}
func (r *Request) GetHttpMethod() string {
	return "POST"
}

func (r *Request) GetType() string {
	return r.t
}

func (r *Request) GetAction() string {
	return r.Action
}

func (r *Request) GetParams() map[string]string {
	return r.params
}

func getUrlQueriesEncoded(params map[string]string) string {
	values := url.Values{}
	for key, value := range params {
		if value != "" {
			values.Add(key, value)
		}
	}
	return values.Encode()
}

func (r *Request) GetBodyReader() io.Reader {
	return strings.NewReader(getUrlQueriesEncoded(r.params))
}

func CompleteCommonParams(request IRequest, region string) {
	params := request.GetParams()
	params["Region"] = region
	params["Action"] = request.GetAction()
	params["Timestamp"] = strconv.FormatInt(time.Now().Unix(), 10)
	params["Nonce"] = strconv.Itoa(rand.Int())
	params["RequestClient"] = "SDK_Golang"
}

// ContactParams 连接参数
func ContactParams(req IRequest) (err error) {
	value := reflect.ValueOf(req)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	err = flatStructure(value, req, "")
	return
}

func flatStructure(value reflect.Value, request IRequest, prefix string) (err error) {
	valueType := value.Type()
	for i := 0; i < valueType.NumField(); i++ {
		tag := valueType.Field(i).Tag
		nameTag, hasNameTag := tag.Lookup("name")
		if !hasNameTag {
			continue
		}
		field := value.Field(i)
		kind := field.Kind()
		if kind == reflect.Ptr && field.IsNil() {
			continue
		}
		if kind == reflect.Ptr {
			field = field.Elem()
			kind = field.Kind()
		}
		key := prefix + nameTag
		if kind == reflect.String {
			s := field.String()
			if s != "" {
				request.GetParams()[key] = s
			}
		} else if kind == reflect.Bool {
			request.GetParams()[key] = strconv.FormatBool(field.Bool())
		} else if kind == reflect.Int || kind == reflect.Int64 {
			request.GetParams()[key] = strconv.FormatInt(field.Int(), 10)
		} else if kind == reflect.Uint || kind == reflect.Uint64 {
			request.GetParams()[key] = strconv.FormatUint(field.Uint(), 10)
		} else if kind == reflect.Float64 {
			request.GetParams()[key] = strconv.FormatFloat(field.Float(), 'f', 4, 64)
		} else if kind == reflect.Slice {
			list := value.Field(i)
			for j := 0; j < list.Len(); j++ {
				vj := list.Index(j)
				key := prefix + nameTag + "." + strconv.Itoa(j)
				kind = vj.Kind()
				if kind == reflect.Ptr && vj.IsNil() {
					continue
				}
				if kind == reflect.Ptr {
					vj = vj.Elem()
					kind = vj.Kind()
				}
				if kind == reflect.String {
					request.GetParams()[key] = vj.String()
				} else if kind == reflect.Bool {
					request.GetParams()[key] = strconv.FormatBool(vj.Bool())
				} else if kind == reflect.Int || kind == reflect.Int64 {
					request.GetParams()[key] = strconv.FormatInt(vj.Int(), 10)
				} else if kind == reflect.Uint || kind == reflect.Uint64 {
					request.GetParams()[key] = strconv.FormatUint(vj.Uint(), 10)
				} else if kind == reflect.Float64 {
					request.GetParams()[key] = strconv.FormatFloat(vj.Float(), 'f', 4, 64)
				} else if kind == reflect.Struct {
					j, e := json.Marshal(field.Interface())
					if e != nil {
						return e
					}
					request.GetParams()[key] = string(j)
				} else {
					flatStructure(vj, request, key+".")
				}
			}
		} else if kind == reflect.Struct {
			j, e := json.Marshal(field.Interface())
			if e != nil {
				return e
			}
			request.GetParams()[key] = string(j)
		} else {
			flatStructure(reflect.ValueOf(field.Interface()), request, prefix+nameTag+".")
		}
	}
	return
}
