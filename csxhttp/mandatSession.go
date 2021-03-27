package csxhttp

import (
	"errors"
	"net/http"
	"strings"

	"github.com/Masterminds/semver"
	"github.com/gorilla/sessions"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"
	"gitlab.com/battler/modules/csxaccess"
	"gitlab.com/battler/modules/csxsession"
)

type ControlVersionInfo struct {
	Version         string
	Domains         map[string]interface{}
	AllowedVersions map[string]string
}

// MandatSession struct for manipulate access to http methods
type MandatSession struct {
	accessManager       *csxaccess.AccessManager
	defaultRoles        map[string]interface{}
	sessionStore        *csxsession.CsxStore
	sessionKey          string
	passCheckAuthMap    map[string]bool
	passCheckVersionMap map[string]bool
	passLogMap          map[string]bool
	login               func(ctx echo.Context)
}

// NewMandatSession create struct for manipulate access to http methods
func NewMandatSession(sessionStore *csxsession.CsxStore, sessionKey string, passCheckAuthMap, passCheckVersionMap, passLogMap map[string]bool, accessMandats []*csxaccess.Mandat, categorizer func(mandat *csxaccess.Mandat) (string, string), login func(ctc echo.Context)) *MandatSession {
	mandatSession := MandatSession{
		defaultRoles:        map[string]interface{}{},
		sessionStore:        sessionStore,
		sessionKey:          sessionKey,
		passCheckAuthMap:    passCheckAuthMap,
		passCheckVersionMap: passCheckVersionMap,
		passLogMap:          passLogMap,
		login:               login,
	}
	accessManager := csxaccess.NewAccessManager(categorizer)
	accessManager.Load(accessMandats)
	mandatSession.accessManager = accessManager
	return &mandatSession

}

// SetDefaultRole get session from store
func (mandatSession *MandatSession) SetDefaultRole(roles map[string]interface{}) {
	mandatSession.defaultRoles = roles
}

// GetSession get session from store
func (mandatSession *MandatSession) GetSession(ctx *Context) (*sessions.Session, error) {
	return mandatSession.sessionStore.Get(ctx.Request(), mandatSession.sessionKey)
}

// CheckAccess check mandat access
func (mandatSession *MandatSession) CheckAccess(ctx *Context, info *ControlVersionInfo) (success bool, httpCode int, msg string) {
	var locations, account, firm, ips, clientAppServer, id, token, phone string
	var userStatus int
	var err error
	// var s *sessions.Session
	var session *sessions.Session

	route := ctx.Request().Method + ctx.Path()
	if mandatSession.passCheckAuthMap != nil {
		// no check auth for login methods
		if _, ok := mandatSession.passCheckAuthMap[route]; ok {
			return true, 200, msg
		}
	}

	// default user role
	roles := mandatSession.defaultRoles

	authStatus := mandatSession.setAuthTokenHeader(ctx)
	if authStatus < 0 {
		return false, 401, msg
	} else if authStatus == 0 {
		session, err = mandatSession.GetSession(ctx)
		if err != nil || session.Values == nil {
			return false, 401, msg
		}
		id, ok := session.Values["id"]
		if ok {
			rolesInt := session.Values["roles"]
			roles, _ = rolesInt.(map[string]interface{})
			locations, _ = session.Values["locations"].(string)
			account, _ = session.Values["account"].(string)
			firm, _ = session.Values["firm"].(string)
			ips, _ = session.Values["allowedIP"].(string)
			phone, _ = session.Values["phone"].(string)
			clientAppServer, _ = session.Values["appServer"].(string)
			token, _ = session.Values["token"].(string)
			if session.Values["status"] != nil {
				userStatus = int(session.Values["status"].(float64))
			}
			ctx.Set("id", id)
			ctx.Set("session", session)
			ctx.Set("locations", locations)
			ctx.Set("firm", firm)
			ctx.Set("account", account)
			ctx.Set("token", token)
			ctx.Set("status", userStatus)
			ctx.Set("phone", phone)
		}
	} else {
		return true, 200, ""
	}
	isSuperUser := userStatus == 1
	ctx.Set("roles", roles)

	// ignore mandats for super user
	if !isSuperUser {
		mandats, ok := mandatSession.accessManager.GetMandatsBySubject(route, roles)
		if !ok {
			return false, 401, msg
		}
		if len(mandats) == 0 {
			return false, 401, msg
		}
		mandat := mandats[0]
		if mandat.Params != nil && !mandat.CheckMandatParams(ctx.SimpleQueryParams()) {
			return false, 401, msg
		}
	}

	// check redirect
	needRedirect, redirectTo := CheckNeedRedirect(ctx, clientAppServer, id)
	if needRedirect {
		ctx.Redirect(http.StatusTemporaryRedirect, redirectTo) // HTTP 307
		msg = "redirect"
		return false, http.StatusTemporaryRedirect, msg
	}
	// check app version
	if info != nil {
		success, err := mandatSession.checkVersion(ctx, info)
		if !success {
			var message string
			if err != nil {
				message = err.Error()
			}
			return success, 403, message
		}
	}
	// check allowed ips
	if ips != "" && !strings.Contains(ips, ctx.Request().RemoteAddr) {
		return false, 403, msg
	}

	return true, 200, ""
}

func (mandatSession *MandatSession) checkVersion(ctx echo.Context, info *ControlVersionInfo) (success bool, err error) {
	if info == nil {
		return false, errors.New("InvalidVersionParams")
	}
	route := ctx.Path()
	if _, ok := mandatSession.passCheckVersionMap[route]; ok {
		return true, nil
	}
	osType := ctx.QueryParam("os")
	domain := ctx.Request().Host
	if info.Domains != nil && len(info.Domains) > 0 {
		_, ok := info.Domains[domain]
		if !ok {
			return false, errors.New("InvalidDomain")
		}
	}

	var allowedVersion string
	allowedVersion, ok := info.AllowedVersions[osType]
	if ok {
		c, err := semver.NewConstraint(">= " + allowedVersion)
		if err == nil {
			v, _ := semver.NewVersion(info.Version)
			if v == nil || !c.Check(v) {
				logrus.Debug("invalid app version:" + info.Version + " need:" + allowedVersion)
			} else {
				success = true
			}
		} else {
			logrus.Error("invalid allowed app version:" + allowedVersion)
		}

		if !success {
			return false, errors.New("InvalidAppVersion")
		}
	}
	return true, nil
}

func (mandatSession *MandatSession) setAuthTokenHeader(ctx echo.Context) int {
	authHeader := ctx.Request().Header.Get("Authorization")
	if authHeader == "" || mandatSession.login == nil {
		return 0
	}

	authHeaderParts := strings.Split(authHeader, " ")
	if len(authHeaderParts) != 2 || strings.ToLower(authHeaderParts[0]) != "bearer" {
		logrus.Warn("Authorization header format must be Bearer {token}")
		return -1
	}

	// _, ok := UsersMap.Load(authHeaderParts[1])
	// if ok {
	// 	ctx.Set("AUTH-TOKEN", authHeaderParts[1])
	// 	mandatSession.login(ctx) //TODO :: need understand and refactor
	// 	return 1
	// }
	logrus.Warn("User with token is not exists")
	return -1
}

// CheckNeedRedirect check need redirect to another app server
func CheckNeedRedirect(ctx echo.Context, host string, id string) (bool, string) {
	request := ctx.Request()
	if len(host) == 0 {
		return false, ""
	}
	var scheme string
	if request.TLS == nil {
		scheme = "http"
	} else {
		scheme = "https"
	}
	requestHost := scheme + "://" + request.Host
	if host != "" && host != requestHost {
		if requestHost != "" {
			redirectTo := host + request.URL.Path + "?" + request.URL.RawQuery
			logrus.WithFields(map[string]interface{}{"route": 123, "from:": requestHost, "to:": redirectTo}).Info("client " + id + " redirected")
			return true, redirectTo
		}
		logrus.Error("Request host is missing! Redirection of client " + id + " to " + host + " is not performed")
	}
	return false, ""
}
