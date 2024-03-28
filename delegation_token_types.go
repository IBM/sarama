package sarama

import "time"

type DelegationToken struct {
	Owner       Principal
	Requester   Principal
	IssueTime   time.Time
	ExpiryTime  time.Time
	MaxLifeTime time.Time
	TokenID     string
	HMAC        []byte
}

func (d *DelegationToken) encode(pe packetEncoder, version int16) error {
	if err := d.Owner.encode(pe, version); err != nil {
		return err
	}
	if version > 2 {
		if err := d.Requester.encode(pe, version); err != nil {
			return err
		}
	}
	pe.putInt64(d.IssueTime.UnixMilli())
	pe.putInt64(d.ExpiryTime.UnixMilli())
	pe.putInt64(d.MaxLifeTime.UnixMilli())

	if version < 2 {
		if err := pe.putString(d.TokenID); err != nil {
			return err
		}
		if err := pe.putBytes(d.HMAC); err != nil {
			return err
		}
	} else {
		if err := pe.putCompactString(d.TokenID); err != nil {
			return err
		}
		if err := pe.putCompactBytes(d.HMAC); err != nil {
			return err
		}
	}
	return nil
}

func (d *DelegationToken) decode(pd packetDecoder, version int16) (err error) {
	if err := d.Owner.decode(pd, version); err != nil {
		return err
	}
	if version > 2 {
		if err := d.Requester.decode(pd, version); err != nil {
			return err
		}
	}

	for _, f := range []*time.Time{&d.IssueTime, &d.ExpiryTime, &d.MaxLifeTime} {
		if ms, err := pd.getInt64(); err == nil {
			*f = time.UnixMilli(ms)
		} else {
			return err
		}
	}

	if version < 2 {
		if d.TokenID, err = pd.getString(); err != nil {
			return err
		}
		if d.HMAC, err = pd.getBytes(); err != nil {
			return err
		}
	} else {
		if d.TokenID, err = pd.getCompactString(); err != nil {
			return err
		}
		if d.HMAC, err = pd.getCompactBytes(); err != nil {
			return err
		}
	}

	return nil
}

type Principal struct {
	PrincipalType string
	Name          string
}

func (k *Principal) encode(pe packetEncoder, version int16) (err error) {
	f := func(s string) error {
		if version < 2 {
			return pe.putString(s)
		}
		return pe.putCompactString(s)
	}

	if err = f(k.PrincipalType); err == nil {
		err = f(k.Name)
	}
	return err
}

func (k *Principal) decode(pd packetDecoder, version int16) (err error) {
	f := func() (string, error) {
		if version < 2 {
			return pd.getString()
		}
		return pd.getCompactString()
	}

	if k.PrincipalType, err = f(); err == nil {
		k.Name, err = f()
	}
	return err
}
