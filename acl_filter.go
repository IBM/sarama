package sarama

//ACLFilter is an ACL filter type
type ACLFilter struct {
	ResourceName              *string
	Principal                 *string
	Host                      *string
	Version                   int
	ResourceType              ACLResourceType
	ResourcePatternTypeFilter ACLResourcePatternType
	Operation                 ACLOperation
	PermissionType            ACLPermissionType
}

func (a *ACLFilter) encode(pe packetEncoder) error {
	pe.putInt8(int8(a.ResourceType))
	if err := pe.putNullableString(a.ResourceName); err != nil {
		return err
	}

	if a.Version == 1 {
		pe.putInt8(int8(a.ResourcePatternTypeFilter))
	}

	if err := pe.putNullableString(a.Principal); err != nil {
		return err
	}
	if err := pe.putNullableString(a.Host); err != nil {
		return err
	}
	pe.putInt8(int8(a.Operation))
	pe.putInt8(int8(a.PermissionType))

	return nil
}

func (a *ACLFilter) decode(pd packetDecoder, version int16) (err error) {
	resourceType, err := pd.getInt8()
	if err != nil {
		return err
	}
	a.ResourceType = ACLResourceType(resourceType)

	if a.ResourceName, err = pd.getNullableString(); err != nil {
		return err
	}

	if a.Version == 1 {
		pattern, err := pd.getInt8()

		if err != nil {
			return err
		}

		a.ResourcePatternTypeFilter = ACLResourcePatternType(pattern)
	}

	if a.Principal, err = pd.getNullableString(); err != nil {
		return err
	}

	if a.Host, err = pd.getNullableString(); err != nil {
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
