package terminalCommands

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

// Sender ---
type Sender struct {
	http        *http.Client
	terminalURL *string
}

// TerminalResponse ---
type TerminalResponse struct {
	Id          string                 `json:"id"`
	Result      int32                  `json:"result"`
	Errors      []CommandError         `json:"errors"`
	Telemetry   map[string]interface{} `json:"telemetry"`
	Driver      string                 `json:"driver"`
	Device      string                 `json:"device"`
	VersionSoft int                    `json:"version_soft"`
	VersionHard int                    `json:"version_hard"`
}

// CommandAction ---
type CommandAction struct {
	Id    string `json:"id"`
	Index uint32 `json:"index"`
	Act   uint32 `json:"act"`
	Ton   uint32 `json:"ton"`
	Toff  uint32 `json:"toff"`
}

// Command ---
type Command struct {
	Id      string        `json:"id"`
	Target  string        `json:"target"`
	Command CommandAction `json:"command"`
	Timeout int           `json:"timeout"`
}

// CommandError ---
type CommandError struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
}

const (
	// belka compatible
	guard_error_ign    = 0x000001
	guard_error_park   = 0x000002
	guard_error_doors  = 0x000004
	guard_error_trunk  = 0x000008
	guard_error_space  = 0x000010
	guard_error_hood   = 0x000020
	guard_error_lights = 0x000040
	guard_error_busy   = 0x000080

	// flex extended
	guard_error_sensor    = 0x000100
	guard_error_guard     = 0x000200
	guard_error_can_park  = 0x000400
	guard_error_can_brake = 0x000800
	guard_error_already   = 0x100000
	guard_error_timeout   = 0x200000
	guard_error_disable   = 0x400000
	guard_error_other     = 0x800000

	// common
	command_error_other        = guard_error_other
	command_error_timeout      = guard_error_timeout
	command_error_disable      = guard_error_disable
	command_error_invalid_args = 0x1000000
	command_error_invalid_crc  = 0x2000000
)

// ErrorCodes ---
var ErrorCodes = map[int32]string{
	1100: "TurnOffIgnition",
	1101: "TurnOnParking",
	1102: "CloseDoors",
	1103: "CloseTrunk",
	1104: "VolumeSensor",
	1105: "CloseHood",
	1106: "TurnOffLight",
	1107: "CommandRunning",
	1108: "SensorError",
	1109: "GuardError",
	1110: "BrakeError",
	1111: "CommandDisabled",
	1112: "OtherError",
	-101: "ServiceUnavailable",
	-102: "InvalidResponse",
	-1:   "fail",
	-2:   "notimpl",
	-3:   "CommandTimeout",
	-4:   "invalid_arg",
	-5:   "invalid_cheksum",
	-6:   "low_data",
	-7:   "invalid_format",
	-8:   "terminate",
	-11:  "CommandTimeout",
}

// SetError ---
func (response *TerminalResponse) SetError(code int32) {
	if response.Errors == nil {
		response.Errors = make([]CommandError, 0)
	}
	response.Errors = append(response.Errors, CommandError{code, ErrorCodes[code]})
	response.Result = -1
}

// Run send command request and wait answer
func (sender *Sender) Run(obj string, drv string, action *CommandAction, timeout ...int) (response TerminalResponse) {
	if drv != "" {
		obj = drv + ":" + obj
	}
	cmd := Command{
		Id:      uuid.Must(uuid.NewV4()).String(),
		Target:  obj,
		Command: *action,
	}
	if len(timeout) > 0 {
		cmd.Timeout = timeout[0]
	}
	b, err := json.Marshal(cmd)
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

// Protection ---
func (sender *Sender) Protection(obj string, drv string, on uint8) TerminalResponse {
	action := CommandAction{
		Id:  "guard",
		Act: uint32(on),
	}

	return sender.Run(obj, drv, &action)
}

// Engine ---
func (sender *Sender) Engine(obj string, drv string, on uint8) TerminalResponse {
	action := CommandAction{
		Id:  "engine",
		Act: uint32(on),
	}

	return sender.Run(obj, drv, &action)
}

// Relay ---
func (sender *Sender) Relay(obj string, drv string, idx uint16, on uint8, ton uint32, toff uint32) TerminalResponse {
	action := CommandAction{
		Id:    "relay",
		Index: uint32(idx),
		Act:   uint32(on),
		Ton:   ton,
		Toff:  toff,
	}
	return sender.Run(obj, drv, &action)
}

// State ---
func (sender *Sender) State(obj string, drv string) TerminalResponse {
	action := CommandAction{
		Id: "state",
	}
	return sender.Run(obj, drv, &action)
}

// Reset ---
func (sender *Sender) Reset(obj string, drv string) TerminalResponse {
	action := CommandAction{
		Id: "reset",
	}
	return sender.Run(obj, drv, &action)
}

// SetBitErrors ---
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

	//already guard
	if (*result & guard_error_already) != 0 {
		//response.Errors = append(response.Errors, CommandError{1107, ErrorCodes[1107]})
	}
	//error sensor
	if (*result & guard_error_sensor) != 0 {
		response.Errors = append(response.Errors, CommandError{1108, ErrorCodes[1108]})
	}
	//error guard
	if (*result & guard_error_guard) != 0 {
		response.Errors = append(response.Errors, CommandError{1109, ErrorCodes[1109]})
	}
	//error can park
	if (*result & guard_error_can_park) != 0 {
		response.Errors = append(response.Errors, CommandError{1101, ErrorCodes[1101]})
	}

	//error can brake
	if (*result & guard_error_can_brake) != 0 {
		response.Errors = append(response.Errors, CommandError{1110, ErrorCodes[1110]})
	}

	//error timeout
	if (*result & guard_error_timeout) != 0 {
		response.Errors = append(response.Errors, CommandError{-3, ErrorCodes[-3]})
	}

	//error disabled
	if (*result & guard_error_disable) != 0 {
		response.Errors = append(response.Errors, CommandError{1111, ErrorCodes[1111]})
	}

	//error other
	if (*result & guard_error_other) != 0 {
		response.Errors = append(response.Errors, CommandError{1112, ErrorCodes[1112]})
	}

}

// GetErrorsText ---
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

// NewSender ---
func NewSender(url string) *Sender {
	return &Sender{
		http:        &http.Client{Timeout: 40000000000},
		terminalURL: &url,
	}
}
