package sarama

//Resource holds information about acl resource type
type Resource struct {
	ResourceType        ACLResourceType
	ResourceName        string
	ResourcePatternType ACLResourcePatternType
}

func (r *Resource) encode(pe packetEncoder, version int16) error {
	pe.putInt8(int8(r.ResourceType))

	if err := pe.putString(r.ResourceName); err != nil {
		return err
	}

	if version == 1 {
		if r.ResourcePatternType == ACLPatternUnknown {
			Logger.Print("Cannot encode an unknown resource pattern type, using Literal instead")
			r.ResourcePatternType = ACLPatternLiteral
		}
		pe.putInt8(int8(r.ResourcePatternType))
	}

	return nil
}

func (r *Resource) decode(pd packetDecoder, version int16) (err error) {
	resourceType, err := pd.getInt8()
	if err != nil {
		return err
	}
	r.ResourceType = ACLResourceType(resourceType)

	if r.ResourceName, err = pd.getString(); err != nil {
		return err
	}
	if version == 1 {
		pattern, err := pd.getInt8()
		if err != nil {
			return err
		}
		r.ResourcePatternType = ACLResourcePatternType(pattern)
	}

	return nil
}

//ACL holds information about acl type
type ACL struct {
	Principal      string
	Host           string
	Operation      ACLOperation
	PermissionType ACLPermissionType
}

func (a *ACL) encode(pe packetEncoder) error {
	if err := pe.putString(a.Principal); err != nil {
		return err
	}

	if err := pe.putString(a.Host); err != nil {
		return err
	}

	pe.putInt8(int8(a.Operation))
	pe.putInt8(int8(a.PermissionType))

	return nil
}

func (a *ACL) decode(pd packetDecoder, version int16) (err error) {
	if a.Principal, err = pd.getString(); err != nil {
		return err
	}

	if a.Host, err = pd.getString(); err != nil {
		return err
	}

	operation, err := pd.getInt8()
	if err != nil {
		return err
	}
	a.Operation = ACLOperation(operation)

	permissionType, err := pd.getInt8()
	if err != nil {
		return err
	}
	a.PermissionType = ACLPermissionType(permissionType)

	return nil
}

//ResourceAcls is an acl resource type
type ResourceAcls struct {
	Resource
	Acls []*ACL
}

func (r *ResourceAcls) encode(pe packetEncoder, version int16) error {
	if err := r.Resource.encode(pe, version); err != nil {
		return err
	}

	if err := pe.putArrayLength(len(r.Acls)); err != nil {
		return err
	}
	for _, acl := range r.Acls {
		if err := acl.encode(pe); err != nil {
			return err
		}
	}

	return nil
}

func (r *ResourceAcls) decode(pd packetDecoder, version int16) error {
	if err := r.Resource.decode(pd, version); err != nil {
		return err
	}

	n, err := pd.getArrayLength()
	if err != nil {
		return err
	}

	r.Acls = make([]*ACL, n)
	for i := 0; i < n; i++ {
		r.Acls[i] = new(ACL)
		if err := r.Acls[i].decode(pd, version); err != nil {
			return err
		}
	}

	return nil
}
