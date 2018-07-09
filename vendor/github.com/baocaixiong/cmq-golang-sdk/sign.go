package cmq

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"
	"github.com/baocaixiong/cmq-golang-sdk/models"
)

const (
	SHA256 = "HmacSHA256"
	SHA1   = "HmacSHA1"
)

func signRequest(request models.IRequest, host string, credential *Credential, method string) (err error) {
	if method != SHA256 {
		method = SHA1
	}
	checkAuthParams(request, credential, method)
	s := getStringToSign(request)
	s = fmt.Sprintf("%s%s%s?%s", request.GetHttpMethod(), host, UriSec, s)
	signature := sign(s, credential, method)
	request.GetParams()["Signature"] = signature
	return
}

func sign(s string, c *Credential, method string) string {
	hashed := hmac.New(sha1.New, []byte(c.SecretKey))
	if method == SHA256 {
		hashed = hmac.New(sha256.New, []byte(c.SecretKey))
	}
	hashed.Write([]byte(s))

	return base64.StdEncoding.EncodeToString(hashed.Sum(nil))
}

func checkAuthParams(request models.IRequest, credential *Credential, method string) {
	params := request.GetParams()
	params["SignatureMethod"] = method
	params["SecretId"] = credential.SecretId
	delete(params, "Signature")
}

func getStringToSign(request models.IRequest) string {
	text := ""
	params := request.GetParams()
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := range keys {
		k := keys[i]
		if params[k] == "" {
			continue
		}
		text += fmt.Sprintf("%v=%v&", strings.Replace(k, "_", ".", -1), params[k])
	}
	text = text[:len(text)-1]
	return text
}
