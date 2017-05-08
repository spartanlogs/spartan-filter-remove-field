package mutate

import (
	"github.com/spartanlogs/spartan/config"
	"github.com/spartanlogs/spartan/event"
	"github.com/spartanlogs/spartan/filters"
	"github.com/spartanlogs/spartan/utils"
)

func init() {
	filters.Register("remove_field", newRemoveFieldFilter)
}

type removeFieldConfig struct {
	fields []string
}

var removeFieldConfigSchema = []config.Setting{
	{
		Name:     "fields",
		Type:     config.Array,
		Required: true,
		ElemType: &config.Setting{Type: config.String},
	},
}

// A RemoveFieldFilter is used to perform several different actions on an Event.
// See the documentation for the Mutate filter for more information.
type RemoveFieldFilter struct {
	config *removeFieldConfig
}

func newRemoveFieldFilter(options utils.InterfaceMap) (filters.Filter, error) {
	options = config.CheckOptionsMap(options)
	f := &RemoveFieldFilter{config: &removeFieldConfig{}}
	if err := f.setConfig(options); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *RemoveFieldFilter) setConfig(options utils.InterfaceMap) error {
	var err error
	options, err = config.VerifySettings(options, removeFieldConfigSchema)
	if err != nil {
		return err
	}

	f.config.fields = options.Get("fields").([]string)

	return nil
}

// Filter processes a batch.
func (f *RemoveFieldFilter) Filter(batch []*event.Event, matchedFunc filters.MatchFunc) []*event.Event {
	for _, event := range batch {
		for _, field := range f.config.fields {
			event.DeleteField(field)
		}
	}
	return batch
}
