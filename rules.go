package calc

import (
	"reflect"
	"strconv"

	"gitlab.com/battler/models"
	"gitlab.com/battler/modules/telemetry"
)

// Rules is a rules for calc events
type Rules []Rule

// Rule is a rule for calc events
type Rule struct {
	ID     *string  `json:"id"`
	Action *string  `json:"act"`
	Value  *float64 `json:"val"`
	Rules  *Rules   `json:"rules"`
}

// RuleContext is a data for check rules
type RuleContext struct {
	Obj   *models.Object
	Cfg   *models.ObjectConfig
	Pos   *telemetry.FlatPosition
	Value *float64
}

// Check is check notification rules
func (context *RuleContext) Check(rules *Rules) bool {
	return context.and(&Rule{Rules: rules})
}

func (context *RuleContext) getValue(id *string) (float64, bool) {
	var v float64
	var ok bool
	params := context.Pos.P
	if id == nil || *id == "" {
		if context.Value == nil {
			ok = false
		} else {
			v = *context.Value
			ok = true
		}
	} else {
		p, err := strconv.Atoi(*id)
		if err == nil {
			v, ok = params[uint16(p)]
		} else {
			ret, err := context.Cfg.CheckState(*id, &params)
			ok := err == nil
			if ok && ret {
				v = 1
			}
		}
		context.Value = &v
	}
	return v, ok
}

func (context *RuleContext) check(rule *Rule) bool {
	if rule.Action == nil {
		return false
	}
	act := reflect.ValueOf(context).MethodByName(*rule.Action)
	if !act.IsValid() {
		return false
	}
	ret := act.Call([]reflect.Value{reflect.ValueOf(rule)})
	return ret[0].Bool()
}

func (context *RuleContext) and(rule *Rule) bool {
	var cnt int
	if rule.Rules != nil {
		cnt = len(*rule.Rules)
	}
	result := false
	for i := 0; i < cnt; i++ {
		rule := (*rule.Rules)[i]
		result = context.check(&rule)
		if !result {
			break
		}
	}
	return result
}

func (context *RuleContext) or(rule *Rule) bool {
	var cnt int
	if rule.Rules != nil {
		cnt = len(*rule.Rules)
	}
	result := false
	for i := 0; i < cnt; i++ {
		rule := (*rule.Rules)[i]
		result = context.check(&rule)
		if result {
			break
		}
	}
	return result
}

func (context *RuleContext) eq(rule *Rule) bool {
	v, ok := context.getValue(rule.ID)
	if ok {
		var r float64
		if rule.Value != nil {
			r = *rule.Value
		}
		ok = v != r
	}
	return ok
}

func (context *RuleContext) lt(rule *Rule) bool {
	v, ok := context.getValue(rule.ID)
	if ok {
		var r float64
		if rule.Value != nil {
			r = *rule.Value
		}
		ok = v < r
	}
	return ok
}

func (context *RuleContext) lte(rule *Rule) bool {
	v, ok := context.getValue(rule.ID)
	if ok {
		var r float64
		if rule.Value != nil {
			r = *rule.Value
		}
		ok = v <= r
	}
	return ok
}

func (context *RuleContext) gt(rule *Rule) bool {
	v, ok := context.getValue(rule.ID)
	if ok {
		var r float64
		if rule.Value != nil {
			r = *rule.Value
		}
		ok = v > r
	}
	return ok
}

func (context *RuleContext) gte(rule *Rule) bool {
	v, ok := context.getValue(rule.ID)
	if ok {
		var r float64
		if rule.Value != nil {
			r = *rule.Value
		}
		ok = v >= r
	}
	return ok
}
