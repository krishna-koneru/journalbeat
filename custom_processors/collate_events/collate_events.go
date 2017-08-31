package collate_events

import (
	"github.com/elastic/beats/libbeat/common"
	p "github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/logp"
	"fmt"
	"time"
)

type eventCounter struct {
	Count int
	LastTimestamp time.Time
}

type collateEvents struct {
	Interval int
	Rules map[string]*p.Condition
	Metrics map[string]*eventCounter
}

func init() {
	p.RegisterPlugin("collate_events",newCollateEvents)
}
// TODO add config option to specify the rate of messages for interval. eg: 1 event/sec is okay.
type collateEventsCfg struct {
	Interval int				`config:"collation_interval_sec"`
	RulesCfg map[string]*common.Config	`config:"rules"`
}

func newCollateEvents(c common.Config) (p.Processor, error) {

	cfg := collateEventsCfg{}
	c.Unpack(&cfg)

	cEvents := collateEvents{
		Interval:cfg.Interval,
		Rules:map[string]*p.Condition{},
		Metrics:map[string]*eventCounter{},
	}

	// construct a map of rule_id -> conditions
	for ruleId, ruleCfg := range cfg.RulesCfg {
		if ruleCfg.HasField("when") {
			sub, err := ruleCfg.Child("when", -1)
			if err != nil {
				return nil, err
			}
			condConfig := p.ConditionConfig{}
			if err := sub.Unpack(&condConfig); err != nil {
				return nil, err
			}
			cond, err := p.NewCondition(&condConfig)
			if err != nil {
				return nil, err
			}
			cEvents.Rules[ruleId] = cond
			cEvents.Metrics[ruleId] = &eventCounter{Count:0, LastTimestamp:time.Now()}
		}
	}
	go cEvents.LogAndResetAllMetrics() // start thread that logs.
	return &cEvents,nil
}


func (f *collateEvents) LogAndResetAllMetrics () {

	tickChannel := time.NewTicker(time.Second * time.Duration(f.Interval)).C
	quit := make(chan struct{})

	for {
		select {
		case <- tickChannel:
			total := 0
			logstr := ""
			for id,m := range f.Metrics {
				if m.Count > 1 {
					logstr += fmt.Sprintf(" collate_events.%s=%d", id, m.Count)
					total += m.Count
				}
				m.Count = 0
				m.LastTimestamp = time.Now()
			}
			logp.Info("%d events collated in last %d seconds.%s ", total, f.Interval, logstr)
		case <- quit:
			return
		}
	}
}

func (f *collateEvents) Run(event common.MapStr) (common.MapStr, error) {

	logp.LogInit(logp.LOG_INFO, "", false, true, []string{"collate_events"})
	sendEvent := true
	for id,r := range f.Rules {
		if r.Check(event) {
			// Event matches a rule. Check if an event already matched in collation interval
			tsStr, err := event.GetValue("@timestamp"); if err != nil {
				logp.Err("%s",err)
				break
			}
			var ts time.Time
			switch tsStr.(type) {
			case common.Time:
				ts, err = time.Parse(time.RFC3339, tsStr.(common.Time).String()) ; if err != nil { //probably the shittiest line of code to be ever written.
				logp.Err("%s", err)
				return event, nil
			}
			case time.Time:
				ts = tsStr.(time.Time)
			case string:
				ts, err = time.Parse(time.RFC3339, tsStr.(string)) ; if err != nil {
				logp.Err("%s", err)
				return event, nil
				}
			default:
				logp.Err("%s", err)
				return event, nil
			}

			if diff := ts.Sub(f.Metrics[id].LastTimestamp); f.Metrics[id].Count < 1 || diff.Nanoseconds() > 1 * 1e9 {
				f.Metrics[id].Count += 1;
				f.Metrics[id].LastTimestamp = ts
			} else {
				// Dont send the event. A matching event has been already sent in the past second.
				f.Metrics[id].Count += 1;
				f.Metrics[id].LastTimestamp = ts
				sendEvent = false
				break; // event collated, skip checking other rules..
			}
		}
	}

	if sendEvent {
		return event, nil
	} else {
		return nil,nil
	}

}

func (f *collateEvents) String() string {
	return "collate_events= " + fmt.Sprintf("Rules: %+v",f.Rules)
}
