package terminalCommands

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

type Sender struct {
	http        *http.Client
	terminalURL *string
}

type TerminalResponse struct {
	Id        string                 `json:"id"`
	Result    int16                  `json:"result"`
	Errors    []CommandError         `json:"error"`
	Telemetry map[string]interface{} `json:"telemetry"`
}

type CommandAction struct {
	Id    string `json:"id"`
	Index uint16 `json:"index"`
	Act   uint8  `json:"act"`
	Ton   uint32 `json:"ton"`
	Toff  uint32 `json:"toff"`
}

type Command struct {
	Id      string        `json:"id"`
	Target  string        `json:"target"`
	Command CommandAction `json:"command"`
}
type CommandError struct {
	Code    int16  `json:"code"`
	Message string `json:"message"`
}

var ErrorCodes = map[int16]string{
	1100: "Ignition",
	1101: "Parking",
	1102: "Doors",
	1103: "Trunk",
	1104: "Volume sensor",
	1105: "Hood",
	1106: "Lights",
	1107: "Command already running",
	-101: "service unavailable",
	-102: "invalid command response",
	-1:   "fail",
	-2:   "notimpl",
	-3:   "timeout",
	-4:   "invalid_arg",
	-5:   "invalid_cheksum",
	-6:   "low_data",
	-7:   "invalid_format",
	-8:   "terminate",
}

func (response *TerminalResponse) SetError(code int16) {
	if response.Errors == nil {
		response.Errors = make([]CommandError, 0)
	}
	response.Errors = append(response.Errors, CommandError{code, ErrorCodes[code]})
	response.Result = -1
}

func (sender *Sender) response(obj *string, action *CommandAction) (response TerminalResponse) {
	cmd := Command{
		Id:      uuid.Must(uuid.NewV4()).String(),
		Target:  *obj,
		Command: *action,
	}
	b, err := json.Marshal(cmd)
	if err != nil {
		log.Error(err)
	}
	if err != nil {
		log.Error(err)
		response.Result = -1
	} else {
		r := bytes.NewReader(b)
		resp, err := sender.http.Post(*sender.terminalURL, "application/json", r)
		if err != nil {
			log.Error(err)
			response.SetError(-101)
		} else {
			decoder := json.NewDecoder(resp.Body)
			err := decoder.Decode(&response)
			if err != nil {
				log.Error(err)
				response.SetError(-102)
			} else if response.Result < 0 {
				response.SetError(response.Result)
			} else {
				response.SetBitErrors()
			}
			log.Debug(response)
			log.Info("cmd response ", resp)
		}
	}
	return response
}

func (sender *Sender) Protection(obj *string, on uint8) TerminalResponse {
	action := CommandAction{
		Id:  "guard",
		Act: on,
	}

	return sender.response(obj, &action)
}

func (sender *Sender) Engine(obj *string, on uint8) TerminalResponse {
	action := CommandAction{
		Id:  "engine",
		Act: on,
	}

	return sender.response(obj, &action)
}

func (sender *Sender) Relay(idx uint16, obj *string, on uint8, ton uint32, toff uint32) TerminalResponse {
	action := CommandAction{
		Id:    "relay",
		Index: idx,
		Act:   uint8(on),
		Ton:   ton,
		Toff:  toff,
	}
	return sender.response(obj, &action)
}

func (sender *Sender) State(obj *string) TerminalResponse {
	action := CommandAction{
		Id: "state",
	}
	return sender.response(obj, &action)
}

func (sender *Sender) Reset(obj *string) TerminalResponse {
	action := CommandAction{
		Id: "reset",
	}
	return sender.response(obj, &action)
}

func (response *TerminalResponse) SetBitErrors() {
	if response.Errors == nil {
		response.Errors = make([]CommandError, 0)
	}
	result := &response.Result
	//ignition
	if (*result & (1 << 0)) != 0 {
		response.Errors = append(response.Errors, CommandError{1100, ErrorCodes[1100]})
	}
	//parking
	if (*result & (1 << 1)) != 0 {
		response.Errors = append(response.Errors, CommandError{1101, ErrorCodes[1101]})
	}
	//doors
	if (*result & (1 << 2)) != 0 {
		response.Errors = append(response.Errors, CommandError{1102, ErrorCodes[1102]})
	}
	//trunk
	if (*result & (1 << 3)) != 0 {
		response.Errors = append(response.Errors, CommandError{1103, ErrorCodes[1103]})
	}
	//volume sensor
	if (*result & (1 << 4)) != 0 {
		response.Errors = append(response.Errors, CommandError{1104, ErrorCodes[1104]})
	}
	//hood
	if (*result & (1 << 5)) != 0 {
		response.Errors = append(response.Errors, CommandError{1105, ErrorCodes[1105]})
	}
	//lights
	if (*result & (1 << 6)) != 0 {
		response.Errors = append(response.Errors, CommandError{1106, ErrorCodes[1106]})
	}
	//already running
	if (*result & (1 << 7)) != 0 {
		response.Errors = append(response.Errors, CommandError{1107, ErrorCodes[1107]})
	}
}

func (response *TerminalResponse) GetErrorsText() string {
	res := "<unknown>"
	if len(response.Errors) == 0 {
		return res
	} else {
		strArray := make([]string, 0)
		for _, val := range response.Errors {
			strArray = append(strArray, val.Message)
		}
		return strings.Join(strArray, ",")
	}
}

func NewSender(url string) Sender {
	sender := Sender{
		http:        &http.Client{},
		terminalURL: &url,
	}
	return sender
}
