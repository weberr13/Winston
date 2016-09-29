package discover

import (
	// log "github.com/cihub/seelog"
	consul "github.com/hashicorp/consul/api"
	"time"
)

//Discover is the api to find services.
type Discover struct {
	cli *consul.Client
}

//Service is the description of a service.
type Service struct {
	Name string
	Addr string
	Port int
	Tags []string
}

//ServiceRequest allows you to set the options for your request.
type ServiceRequest struct {
	Name           string
	Tag            string
	IncludeFailing bool
	AllowStale     bool
	Near           string //Near requires a string to sort near a service _agent is used for sorting near yourself
	DataCenter     string
	Inconsistent   bool //this will make the read inconsistent but faster
	Token          string
	WaitIndex      uint64
	WaitTime       time.Duration
}

// var EMPTY struct{}

//NewDiscover builds a new Discover object which allows you to find other services.
func NewDiscover() (*Discover, error) {
	client, err := consul.NewClient(consul.DefaultConfig())
	if nil != err {
		return nil, err
	}
	return &Discover{cli: client}, err
}

//Services Returns only healthy services
func (d Discover) Services(s ServiceRequest) ([]*consul.ServiceEntry, error) {
	var passingOnly bool
	var requireConsistent bool
	passingOnly = !s.IncludeFailing     //default is passingOnly
	requireConsistent = !s.Inconsistent //default is consistent

	opts := consul.QueryOptions{
		AllowStale:        s.AllowStale,
		Near:              s.Near,
		Datacenter:        s.DataCenter,
		RequireConsistent: requireConsistent,
		Token:             s.Token,
		WaitIndex:         s.WaitIndex,
		WaitTime:          s.WaitTime,
	}

	entry, _, err := d.cli.Health().Service(s.Name, s.Tag, passingOnly, &opts)
	return entry, err
}

//Close discover.
func (d *Discover) Close() error {
	return nil
}
