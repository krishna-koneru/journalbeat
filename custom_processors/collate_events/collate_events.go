package collate_events

import (
	"fmt"
	"time"
	"strings"
	"github.com/elastic/beats/libbeat/common"
	p "github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
)

type eventCounter struct {
	Count         int
	ServiceNames  map[string]int // number of times a service matched the rule.
	LastTimestamp time.Time
}

type collateEvents struct {
	Interval int
	Rules    map[string]*p.Condition
	Metrics  map[string]*eventCounter
	Events   map[string]common.MapStr
}

// this is set by journalbeat
var Pub publisher.Publisher

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
		Events:map[string]common.MapStr{},
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
			cEvents.Metrics[ruleId] = &eventCounter{Count:0, ServiceNames:map[string]int{}, LastTimestamp:time.Now()}
		}
	}
	go cEvents.LogAndResetAllMetrics() // start thread that logs.
	logp.Info("%s", cEvents.String())
	return &cEvents,nil
}


func (f *collateEvents) LogAndResetAllMetrics () {


	tickChannel := time.NewTicker(time.Second * time.Duration(f.Interval)).C
	quit := make(chan struct{})

	// Publisher is set up after processers are loaded.
	// wait for publisher to be setup.
	for i :=0 ; i < 10 ; i ++ {
		if Pub != nil {
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	var Client publisher.Client
	if Pub != nil {
		Client = Pub.Connect()
	} else {
		Client = nil
	}
	for {
		select {
		case <- tickChannel:
			total := 0
			var events []common.MapStr
			for id,m := range f.Metrics {
				if m.Count > 1 {
					if len(m.ServiceNames) > 1 {
						str := ""
						for n,c := range m.ServiceNames {
							str += fmt.Sprintf("%s (%d times), ", n, c)
						}
						logp.Warn("collation rule %s matches messages from multiple systemd_units : %s", id, str)
					} else {
						event := f.Events[id]
						event.Put("@collated_event", true)
						event["@timestamp"] = time.Now()
						event["@realtime_timestamp"] = time.Now().UnixNano()
						event["message"] = fmt.Sprintf("Collated %d messages in the last %ds for rule: rule_id=%s msg='%s'.", m.Count, f.Interval, id, event["message"])
						events = append(events, event)
					}
					f.Events[id] = common.MapStr{} // reset
					total += m.Count
				}
				m.Count = 0
				m.ServiceNames = map[string]int{}
				m.LastTimestamp = time.Now()
			}

			if Client != nil {
				Client.PublishEvents(events, publisher.Metadata(common.MapStr{}))
			} else if len(events) > 0 {
				logp.Err("could not publish %d collated events" , len(events))
			}
		case <- quit:
			return
		}
	}
}

func (f *collateEvents) Run(event common.MapStr) (common.MapStr, error) {

	if summary,_ := event.HasKey("@collated_event"); summary { return event,nil }
	logp.LogInit(logp.LOG_INFO, "", false, true, []string{"collate_events"})
	sendEvent := true
	for id,r := range f.Rules {
		if r.Check(event) {
			// Event matches a rule. Check if an event already matched in collation interval
			// Use @realtime_timestamp unitl journalbeat updates to libbeat 6.x
			// events is a MapStr (in libbeat 5.x), time.Parse takes too much CPU for large number of events.
			ts, err := event.GetValue("@realtime_timestamp"); if err != nil {
				break
			}

			tsTime := time.Unix(0,int64(ts.(int64)))
			var serviceName string

			if diff := tsTime.Sub(f.Metrics[id].LastTimestamp); f.Metrics[id].Count < 1 || diff.Nanoseconds() > 1 * 1e9 {
				f.Metrics[id].Count = 1;
				f.Metrics[id].LastTimestamp = tsTime
				val, err := event.GetValue("journal.systemd_unit"); if err != nil {
					serviceName = "-"
				} else {
					serviceName = val.(string)
				}
				f.Metrics[id].ServiceNames[serviceName] = 1
				f.Events[id] = event
			} else {
				// Dont send the event. A matching event has been already sent in the past second.
				f.Metrics[id].Count += 1;
				f.Metrics[id].LastTimestamp = tsTime
				val, err := event.GetValue("journal.systemd_unit"); if err != nil {
					serviceName = "-"
				} else {
					serviceName = val.(string)
				}
				if _,found := f.Metrics[id].ServiceNames[serviceName] ; !found {
					f.Metrics[id].ServiceNames[serviceName] = 0
				}
				f.Metrics[id].ServiceNames[serviceName] += 1
				sendEvent = false
				break // event collated, skip checking other rules..
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

	ruleIds := make([]string, len(f.Rules))
	i := 0
	for k := range f.Rules {
		ruleIds[i] = k
		i++
	}
	return "collate_events:" + fmt.Sprintf("Active rules=[%s]",strings.Join(ruleIds, ","))
}

