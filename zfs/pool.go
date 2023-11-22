package zfs

import (
	"bufio"
	"fmt"
	"os/exec"
	"strings"
)

// PoolStatus enum contains status text
type PoolStatus string

// PoolKind enum of supported query types
type PoolKind string

const (
	// PoolProps enum entry
	PoolProps PoolKind = `properties`
	// PoolIostat enum entry
	PoolIostat PoolKind = `iostats`
)

const (
	// PoolOnline enum entry
	PoolOnline PoolStatus = `ONLINE`
	// PoolDegraded enum entry
	PoolDegraded PoolStatus = `DEGRADED`
	// PoolFaulted enum entry
	PoolFaulted PoolStatus = `FAULTED`
	// PoolOffline enum entry
	PoolOffline PoolStatus = `OFFLINE`
	// PoolUnavail enum entry
	PoolUnavail PoolStatus = `UNAVAIL`
	// PoolRemoved enum entry
	PoolRemoved PoolStatus = `REMOVED`
	// PoolSuspended enum entry
	PoolSuspended PoolStatus = `SUSPENDED`
)

type poolImpl struct {
	name string
	kind PoolKind
}

func (p poolImpl) Name() string {
	return p.name
}

func (p poolImpl) Kind() PoolKind {
	return p.kind
}

func (p poolImpl) Properties(props ...string) (PoolProperties, error) {
	handler := newPoolPropertiesImpl(p.kind)
	switch p.kind {
	case PoolProps:
		if err := execute(p.name, handler, `zpool`, `get`, `-Hpo`, `name,property,value`, strings.Join(props, `,`)); err != nil {
			return handler, err
		}
	case PoolIostat:
		if err := execute(p.name, handler, `zpool`, `iostat`, `-Hyp`); err != nil {
			return handler, err
		}
	default:
		return handler, fmt.Errorf("unknown pool type: %s hhhhhhhh", p.kind)
	}
	return handler, nil
}

type poolPropertiesImpl struct {
	kind       PoolKind
	properties map[string]string
}

func (p *poolPropertiesImpl) Properties() map[string]string {
	return p.properties
}

// processLine implements the handler interface
func (p *poolPropertiesImpl) processLine(pool string, line []string) error {
	switch p.kind {
	case PoolProps:
		if len(line) != 3 || line[0] != pool {
			return ErrInvalidOutput
		}
		p.properties[line[1]] = line[2]
	case PoolIostat:
		if len(line) != 7 || line[0] != pool {
			return ErrInvalidOutput
		}
		p.properties["opread"] = line[3]
		p.properties["opwrite"] = line[4]
		p.properties["bwwrite"] = line[5]
		p.properties["bwwrite"] = line[6]
	default:
		return fmt.Errorf("unknown pool type: %s xxxxxxx", p.kind)
	}

	return nil
}

// processLine implements the handler interface
func (p *poolPropertiesImpl) getFieldsPerRecord() int {
	switch p.kind {
	case PoolProps:
		return 3
	case PoolIostat:
		return 7
	default:
		return 3
	}
}

// PoolNames returns a list of available pool names
func poolNames() ([]string, error) {
	pools := make([]string, 0)
	cmd := exec.Command(`zpool`, `list`, `-Ho`, `name`)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(out)

	if err = cmd.Start(); err != nil {
		return nil, err
	}

	for scanner.Scan() {
		pools = append(pools, scanner.Text())
	}
	if err = cmd.Wait(); err != nil {
		return nil, err
	}

	return pools, nil
}

func newPoolImpl(name string, kind PoolKind) poolImpl {
	return poolImpl{
		name: name,
		kind: kind,
	}
}

func newPoolPropertiesImpl(pkind PoolKind) *poolPropertiesImpl {
	return &poolPropertiesImpl{
		kind:       pkind,
		properties: make(map[string]string),
	}
}
