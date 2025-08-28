package idcard_sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	zaplogger "github.com/NumberMan1/component/zap-logger"
	"github.com/NumberMan1/component/zap-logger/field"
	utils2 "github.com/NumberMan1/numbox/utils"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Config struct {
	AlibabaConfig AlibabaConfig `json:"alibaba_config" yaml:"alibaba-config"`
}

type IdCardSDK interface {
	// Valid 验证身份证与名字
	Valid(name, idNo string) (checkRes bool, info IdInfo)
}

type IdInfo struct {
	Name     string
	IdNo     string
	Province string
	City     string
	County   string
	Birthday time.Time
	Sex      string
	Age      int32
}

var alibabaIdCardSDKInstance IdCardSDK

func GetAlibabaIdCardSDK() IdCardSDK {
	utils2.Asset(alibabaIdCardSDKInstance != nil, errors.New("AlibabaIdCard sdk not initialized"))
	return alibabaIdCardSDKInstance
}

func InitAlibabaIdCardSDK(config Config) {
	alibabaIdCardSDKInstance = NewAlibabaIdCardSDK(config.AlibabaConfig)
}

type AlibabaConfig struct {
	AppCode string `json:"app_code" yaml:"app-code"`
	Url     string `json:"url" yaml:"url"`
}

type AlibabaIdCardSDK struct {
	config AlibabaConfig
}

func NewAlibabaIdCardSDK(config AlibabaConfig) *AlibabaIdCardSDK {
	return &AlibabaIdCardSDK{config: config}
}

// Valid 通过阿里巴巴SDK验证身份证与名字
func (sdk *AlibabaIdCardSDK) Valid(name, id string) (checkRes bool, info IdInfo) {
	if sdk.config.AppCode == "" {
		return
	}
	formData := url.Values{}
	formData.Set("name", name)
	formData.Set("idNo", id)
	req, err := http.NewRequest("POST", sdk.config.Url, strings.NewReader(formData.Encode()))
	if err != nil {
		zaplogger.DefaultLogger().Error("AlibabaIdCardSDK Valid in http.NewRequest", field.WithError(err))
		return
	}
	// 注意：Authorization 值中的"APPCODE"和后面的代码之间有一个空格
	req.Header.Set("Authorization", fmt.Sprintf("APPCODE %s", sdk.config.AppCode))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		zaplogger.DefaultLogger().Error("AlibabaIdCardSDK Valid in client.Do", field.WithError(err))
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		zaplogger.DefaultLogger().Error("AlibabaIdCardSDK Valid in io.ReadAll", field.WithError(err))
		return
	}

	type validResp struct {
		Name        string `json:"name"`
		IdNo        string `json:"idNo"`
		RespMessage string `json:"respMessage"`
		RespCode    string `json:"respCode"`
		Province    string `json:"province"`
		City        string `json:"city"`
		County      string `json:"county"`
		Birthday    string `json:"birthday"`
		Sex         string `json:"sex"`
		Age         string `json:"age"`
	}
	var data validResp
	err = json.Unmarshal(body, &data)
	checkRes = err == nil && data.RespCode == "0000"
	if !checkRes {
		return
	}
	birthDay, err := time.Parse("20060102", data.Birthday)
	if err != nil {
		zaplogger.DefaultLogger().Error("AlibabaIdCardSDK Valid in time.Parse", field.WithError(err))
		return
	}
	info = IdInfo{
		Name:     data.Name,
		IdNo:     data.IdNo,
		Province: data.Province,
		City:     data.City,
		County:   data.County,
		Birthday: birthDay,
		Sex:      data.Sex,
		Age:      utils2.ParseIntString[int32](data.Age),
	}
	return
}
