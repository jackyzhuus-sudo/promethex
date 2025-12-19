package data

import (
	"encoding/base64"
	"fmt"
	"io"
	"market-backend/internal/pkg/util"
	"net/http"

	"github.com/bitly/go-simplejson"
)

func (r *Data) GetUserInfoFromPrivy(ctx util.Ctx, did string) (*simplejson.Json, error) {
	if did == "" {
		return nil, fmt.Errorf("did is empty")
	}

	url := fmt.Sprintf("https://api.privy.io/v1/users/%s", did)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("privy-app-id", r.PrivyClient.AppId)

	// HTTP Basic Authentication: appid:appsecret -> base64 -> "Basic " + base64
	credentials := r.PrivyClient.AppId + ":" + r.PrivyClient.AppSecret
	encodedCredentials := base64.StdEncoding.EncodeToString([]byte(credentials))
	req.Header.Add("Authorization", "Basic "+encodedCredentials)

	rsp, err := r.PrivyClient.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer rsp.Body.Close()

	respBody, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}
	ctx.Log.Infof("GetUserInfoFromPrivy response body: %s", string(respBody))

	if rsp.StatusCode != 200 {
		ctx.Log.Errorf("GetUserInfoFromPrivy HTTP request failed with status code: %d, body: %s", rsp.StatusCode, respBody)
		return nil, fmt.Errorf("HTTP status code: [%d] body: [%s]", rsp.StatusCode, string(respBody))
	}

	// 解析为simplejson
	rspJson, err := simplejson.NewJson(respBody)
	if err != nil {
		return nil, err
	}

	return rspJson, nil
}
