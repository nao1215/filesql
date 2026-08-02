package filesql

import (
	"sync"

	achconv "github.com/nao1215/filesql/parser/ach"
	wireconv "github.com/nao1215/filesql/parser/wire"
)

// PendingRegistries contains ACH and Fedwire metadata produced by a load.
//
// A loader must publish this metadata only after the transaction containing the
// corresponding tables has committed. Keeping it separate from the process
// registries prevents a rollback from leaving write-back metadata for tables
// that do not exist.
type PendingRegistries struct {
	ach  map[string]*achconv.TableSet
	wire map[string]*wireconv.TableSet
	once sync.Once
}

func newPendingRegistries() *PendingRegistries {
	return &PendingRegistries{
		ach:  make(map[string]*achconv.TableSet),
		wire: make(map[string]*wireconv.TableSet),
	}
}

func (p *PendingRegistries) addACH(baseName string, tableSet *achconv.TableSet) {
	if p != nil {
		p.ach[baseName] = tableSet
	}
}

func (p *PendingRegistries) addWire(baseName string, tableSet *wireconv.TableSet) {
	if p != nil {
		p.wire[baseName] = tableSet
	}
}

// PublishRegistries makes the metadata visible to DumpACH and DumpFedWire.
// Call it only after the transaction that loaded the corresponding tables has
// committed successfully. It is safe to call more than once.
func (p *PendingRegistries) PublishRegistries() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		for baseName, tableSet := range p.ach {
			registerACHTableSet(baseName, tableSet)
		}
		for baseName, tableSet := range p.wire {
			registerWireTableSet(baseName, tableSet)
		}
	})
}
