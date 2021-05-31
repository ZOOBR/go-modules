package csxapitest

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/sessions"
	"github.com/sirupsen/logrus"
	"gitlab.com/battler/models"
	"gitlab.com/battler/modules/csxhandlers"
	"gitlab.com/battler/modules/csxtimers"
	"gitlab.com/battler/modules/sql"
)

const (
	CheckMethodEqual         int = 1
	CheckMethodContainsProps int = 2
	CheckMethodEqualProps    int = 3
)

// Status is a storage for app emulator status
var (
	testedServiceURL string
	Status           AppEmulatorStatus
	scenaries        = sync.Map{}
	clients          = sync.Map{}
	clientsSid       = sync.Map{}
	testFailed       bool
	testCompleeted   bool
	clientsPassed    = 0
	lenClients       = 0
	testCounter      = 0
	previousTime     *time.Time
)

// AppEmulatorConfig struct for set emulator config
type AppEmulatorConfig struct {
	LoadClients      func(*sync.Map)
	LoadScenaries    func(*sync.Map)
	StartScenario    *string
	TestedServiceURL string
}

// AppEmulatorStateRule struct for check state condition
type AppEmulatorStateRule struct {
	RuleType    string                 `json:"ruleType"`
	CheckMap    map[string]interface{} `json:"checkMap"`
	CheckMethod int                    `json:"checkMethod"`
}

//
// type RequestBody struct {
// 	Collection string `json:"collection"`
// 	Key        string `json:"key"`
// }

// AppEmulatorState struct contains state for emulate
type AppEmulatorState struct {
	Name             string                 `json:"name"`
	ID               string                 `json:"id"`
	RequestUrl       string                 `json:"requestUrl"`
	RequestMethod    string                 `json:"requestMethod"`
	Async            bool                   `json:"async"`
	RequestBody      map[string]interface{} `json:"requestBody"`
	ContentType      string                 `json:"contentType"`
	RequestUrlParams map[string]string      `json:"requestUrlParams"`

	SpecialAction string `json:"specialAction"`

	IntervalMin int `json:"intervalMin"`
	IntervalMax int `json:"intervalMax"`

	CheckRule *AppEmulatorStateRule `json:"checkRule"`

	NextScenario string `json:"nextScenario"`
	FailScenario string `json:"failScenario"`

	AfterMinTimeout int `json:"afterMinTimeout"`
	AfterMaxTimeout int `json:"afterMaxTimeout"`

	BeforeMinTimeout int `json:"beforeMinTimeout"`
	BeforeMaxTimeout int `json:"beforeMaxTimeout"`
}

// AppEmulatorScenario struct contains emulator states
type AppEmulatorScenario struct {
	ID     string
	States []*AppEmulatorState `json:"states"`
}

// AppEmulatorStatus is struct for status of app emulator
type AppEmulatorStatus struct {
	Running bool `json:"running"`
}

type TestScenario struct {
	ID   string              `json:"id"`
	Name string              `json:"name"`
	Data AppEmulatorScenario `json:"data"`
}

type RequestInfo struct {
	sync.RWMutex
	Url         string
	Method      string
	UrlParams   map[string]string
	Body        map[string]interface{}
	ContentType string
	SaveCookie  bool
	Client      *models.Client
	Session     *sessions.Session
}

func newRequestInfo(url, method string, client *models.Client) *RequestInfo {
	reqInfo := RequestInfo{
		Url:    url,
		Method: method,
		Client: client,
	}
	return &reqInfo
}

func makeClientRequest(requestInfo *RequestInfo, result interface{}) error {
	fullURL := testedServiceURL + "/" + requestInfo.Url
	jsonStr, err := json.Marshal(requestInfo.Body)
	if err != nil {
		return err
	}

	if len(requestInfo.UrlParams) > 0 {
		fullURL += "?"
		needDelimiter := false
		for key, val := range requestInfo.UrlParams {
			if needDelimiter {
				fullURL += "&" + key + "=" + val
			} else {
				needDelimiter = true
				fullURL += key + "=" + val
			}
		}
	}

	req, err := http.NewRequest(requestInfo.Method, fullURL, bytes.NewBuffer(jsonStr))
	if err != nil {
		// logrus.Error("Error create HTTP request to "+fullURL+": ", err)
		return err
	}
	if requestInfo.ContentType != "" {
		req.Header.Add("Content-Type", requestInfo.ContentType)
	}
	// req.Header.Set("Authorization", "Bearer "+emulatedClientsToken)
	// req.Header.Set("Cookie", cookieString)
	setCookie(req, requestInfo.Client.Id)

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("invalid request status: " + resp.Status)
	}
	if requestInfo.SaveCookie {
		for _, cookie := range resp.Cookies() {
			c := cookie
			clientsSid.Store(requestInfo.Client.Id, c)
		}
	}

	if err != nil {
		//logrus.Error("Error send request to "+fullURL+": ", err)
		return err
	}
	if resp != nil && result != nil {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if len(data) > 0 {
			err := json.Unmarshal(data, &result)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
		}
	}
	return nil
}

func setCookie(req *http.Request, clientID string) error {
	cookiePtr, err := getCookie(clientID)
	if err != nil {
		return err
	}
	if cookiePtr == nil {
		return errors.New("empty cookie for client: " + clientID)
	}
	req.AddCookie(cookiePtr)
	return nil
}

func getCookie(clientID string) (*http.Cookie, error) {
	cookieInt, ok := clientsSid.Load(clientID)
	if !ok {
		return nil, errors.New("client session not found, client: " + clientID)
	}
	cookie, ok := cookieInt.(*http.Cookie)
	if !ok {
		return nil, errors.New("sid interface conversion error, not string")
	}
	return cookie, nil
}

func checkRequestRule(state *AppEmulatorState, result map[string]interface{}) bool {
	rule := state.CheckRule
	if rule != nil {
		if rule.CheckMethod == CheckMethodContainsProps || rule.CheckMethod == CheckMethodEqualProps {
			for key, val := range rule.CheckMap {
				propVal, ok := result[key]
				if !ok {
					return false
				}
				if rule.CheckMethod == CheckMethodEqualProps && propVal != val {
					return false
				}
			}
		}
	}
	return true
}

func (reqInfo *RequestInfo) setRequestBodyParams(state *AppEmulatorState) error {
	var clientMap map[string]interface{}
	client := reqInfo.Client
	clientMap = sql.GetMapFromStruct(client, nil)
	for key := range state.RequestBody {
		if _, ok := clientMap[key]; ok {
			reqInfo.Body[key] = clientMap[key]
		}
	}
	reqInfo.ContentType = state.ContentType
	if state.SpecialAction != "" {
		switch state.SpecialAction {
		case "auth-phone":
			reqInfo.SaveCookie = true
			reqInfo.UrlParams["phone"] = *client.Phone
		case "auth-code":
			// session, err := getSession(*client.Phone)
			req, _ := http.NewRequest("GET", "", nil)
			setCookie(req, client.Id)

			session, err := csxhandlers.GetSessionByRequest(req)
			if err != nil {
				return err
			}
			reqInfo.Session = session
			codeInt, ok := session.Values["smsCode"]
			if !ok {
				return errors.New("code for auth not found in session, client: " + client.Id)
			}
			code, ok := codeInt.(string)
			if !ok {
				return errors.New("code for auth is not string value, client: " + client.Id)
			}
			reqInfo.UrlParams["code"] = code
		}
	}
	return nil
}

func calcClientRequestSrc(deviceToken, phone string) string {
	hours := 3
	timein := time.Now().UTC().Add(time.Hour * time.Duration(hours))
	timeStr := timein.Format("2006-01-02")
	text := timeStr + deviceToken + phone
	hash := md5.Sum([]byte(text))
	serverCRC := hex.EncodeToString(hash[:])
	return serverCRC
}

func random(min, max int) int {
	return rand.Intn(max-min) + min
}

func generateTimeout(min, max int) {
	if min == 0 && max == 0 {
		return
	}
	time.Sleep(time.Duration(time.Duration(random(min, max)) * time.Millisecond))
}

func runScenario(scenario *AppEmulatorScenario, client *models.Client) bool {
	defer func() {
		if clientsPassed == lenClients && !testCompleeted {
			if testFailed {
				logrus.Info("emulate requests ended, test status [FAILED :(]")
			} else {
				testCompleeted = true
				logrus.Info("emulate requests ended, test status [SUCCESS :)]")
			}
		}
	}()
	for i := 0; i < len(scenario.States); i++ {
		if testFailed {
			break
		}
		state := scenario.States[i]
		requestInfo := newRequestInfo(state.RequestUrl, state.RequestMethod, client)
		if len(state.RequestUrlParams) > 0 {
			requestInfo.UrlParams = map[string]string{}
			for k, v := range state.RequestUrlParams {
				requestInfo.UrlParams[k] = v
			}
		}
		serverCRC := calcClientRequestSrc(*client.DeviceToken, *client.Phone)
		requestInfo.UrlParams["p1"] = serverCRC
		requestInfo.setRequestBodyParams(state)
		generateTimeout(state.BeforeMinTimeout, state.BeforeMaxTimeout)
		if state.IntervalMin != 0 || state.IntervalMax != 0 {
			csxtimers.SetIntervalChan(func(clear chan bool) {
				err := makeClientRequest(requestInfo, nil)
				if testFailed {
					return
				}
				if err != nil {
					clear <- true
					testFailed = true
					logrus.Error("test [FAILED] state: ", state.Name, " method: ", requestInfo.Method, " url: ", requestInfo.Url)
				} else {
					passTestState(state.Name, requestInfo.Method, requestInfo.Url, client.Id)
				}
			}, random(state.IntervalMin, state.IntervalMax), true)
		} else if state.Async {
			go func() {
				err := makeClientRequest(requestInfo, nil)
				if testFailed {
					return
				}
				if err != nil {
					testFailed = true
					logrus.Error("test [FAILED] state: ", state.Name, " method: ", requestInfo.Method, " url: ", requestInfo.Url)
				} else {
					passTestState(state.Name, requestInfo.Method, requestInfo.Url, client.Id)
				}
			}()
			logrus.Info("pass async scenario state: " + state.Name)
		} else {
			var result map[string]interface{}
			if state.CheckRule != nil {
				result = map[string]interface{}{}
			}
			err := makeClientRequest(requestInfo, result)
			if testFailed {
				return false
			}
			if err != nil {
				testFailed = true
				logrus.Error("test [FAILED] state: ", state.Name, " method: ", requestInfo.Method, " url: ", requestInfo.Url, " err: ", err)
				break
			} else {
				if checkRequestRule(state, result) {
					passTestState(state.Name, requestInfo.Method, requestInfo.Url, client.Id)
				} else {
					logrus.Info("test [FAIL] state: ", state.Name, " method: ", requestInfo.Method, " url: ", requestInfo.Url, " client: ", client.Id)
				}
			}

		}
		generateTimeout(state.AfterMinTimeout, state.AfterMaxTimeout)
		if state.NextScenario != "" {
			nextScenario := getScenario(state.NextScenario)
			if nextScenario != nil {
				return runScenario(nextScenario, client)
			}
		}
	}
	return true
}

func getScenario(scenarioID string) *AppEmulatorScenario {
	scenarioInt, ok := scenaries.Load(scenarioID)
	if !ok {
		logrus.Error("scenario not found, id: " + scenarioID)
		return nil
	}
	scenario, ok := scenarioInt.(*AppEmulatorScenario)
	if !ok {
		logrus.Error("scenario interface conversion err, id: " + scenarioID)
		return nil
	}
	return scenario
}

func runScenaries(startScenario string) {
	if testFailed {
		return
	}
	scenarioInt, ok := scenaries.Load(startScenario)
	if !ok {
		logrus.Error("test [NOT RUNNING] start scenario: ", startScenario, " not found in map")
	}
	scenario, ok := scenarioInt.(*AppEmulatorScenario)
	if !ok {
		logrus.Error("test [NOT RUNNING] start scenario ", startScenario, " interface conversion err, not *AppEmulatorScenario")
	}
	clients.Range(func(_, clientInt interface{}) bool {
		clientsPassed++
		if testFailed {
			return false
		}
		client, ok := clientInt.(*models.Client)
		if !ok {
			logrus.Error("test [FAILED] runScenaries, invalid client value: <<client, ok := clientInt.(*models.Client)>>")
			return true
		}
		go runScenario(scenario, client)
		generateTimeout(100, 500)
		return true
	})
}

func passTestState(state, method, url, client string) {
	if testCounter%100 == 0 {
		now := time.Now()
		var ms float64
		if previousTime != nil {
			ms = now.Sub(*previousTime).Seconds()
		}
		logrus.Info("test in progress [PASS] <", testCounter, "> (", math.Round(100/ms), " per sec) state: "+state+" method: "+method+" url: "+url+" client: "+client)
		previousTime = &now
	}
	testCounter++
}

func setTestFailed(info string) {
	testFailed = true
	logrus.Error("test [FAILED] ", info)
}

func getClientsCount() int {
	count := 0
	clients.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// StartEmulation starts application emulation
func StartEmulation(config AppEmulatorConfig) {
	// init variables
	testFailed = false
	testCompleeted = false
	testCounter = 0
	clientsPassed = 0

	// apply config params and callbacks
	config.LoadClients(&clients)
	config.LoadScenaries(&scenaries)
	testedServiceURL = config.TestedServiceURL

	logrus.Info("Starting emulation, clients count: ", getClientsCount())
	// run scenaries
	runScenaries(*config.StartScenario)
	Status.Running = true
}

// StopEmulation stops application emulation
func StopEmulation() {
	logrus.Info("Stop emulation")
	Status.Running = false
}
