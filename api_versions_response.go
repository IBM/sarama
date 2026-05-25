package sarama

import (
	"time"
)

// ApiVersionsResponseKey contains the APIs supported by the broker.
type ApiVersionsResponseKey struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ApiKey contains the API index.
	ApiKey int16
	// MinVersion contains the minimum supported version, inclusive.
	MinVersion int16
	// MaxVersion contains the maximum supported version, inclusive.
	MaxVersion int16
}

type ApiVersionsSupportedFeatureKey struct {
	Name       string
	MinVersion int16
	MaxVersion int16
}

type ApiVersionsFinalizedFeatureKey struct {
	Name            string
	MaxVersionLevel int16
	MinVersionLevel int16
}

func (a *ApiVersionsResponseKey) encode(pe packetEncoder, version int16) (err error) {
	a.Version = version
	pe.putInt16(a.ApiKey)

	pe.putInt16(a.MinVersion)

	pe.putInt16(a.MaxVersion)

	if version >= 3 {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}

func (a *ApiVersionsResponseKey) decode(pd packetDecoder, version int16) (err error) {
	a.Version = version
	if a.ApiKey, err = pd.getInt16(); err != nil {
		return err
	}

	if a.MinVersion, err = pd.getInt16(); err != nil {
		return err
	}

	if a.MaxVersion, err = pd.getInt16(); err != nil {
		return err
	}

	_, err = pd.getEmptyTaggedFieldArray()
	return err
}

type ApiVersionsResponse struct {
	// Version defines the protocol version to use for encode and decode
	Version int16
	// ErrorCode contains the top-level error code.
	ErrorCode int16
	// ApiKeys contains the APIs supported by the broker.
	ApiKeys []ApiVersionsResponseKey
	// ThrottleTimeMs contains the duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs         int32
	SupportedFeatures      []ApiVersionsSupportedFeatureKey // v3, tag 0
	FinalizedFeaturesEpoch int64                            // v3, tag 1
	FinalizedFeatures      []ApiVersionsFinalizedFeatureKey // v3, tag 2
}

func (r *ApiVersionsResponse) setVersion(v int16) {
	r.Version = v
}

func (r *ApiVersionsResponse) encode(pe packetEncoder) (err error) {
	pe.putInt16(r.ErrorCode)

	if err := pe.putArrayLength(len(r.ApiKeys)); err != nil {
		return err
	}
	for _, block := range r.ApiKeys {
		if err := block.encode(pe, r.Version); err != nil {
			return err
		}
	}

	if r.Version >= 1 {
		pe.putInt32(r.ThrottleTimeMs)
	}

	if r.Version >= 3 {
		encoders := taggedFieldEncoders{}
		if len(r.SupportedFeatures) > 0 {
			features := r.SupportedFeatures
			encoders[0] = func(pe packetEncoder) error {
				if err := pe.putArrayLength(len(features)); err != nil {
					return err
				}
				for _, f := range features {
					if err := pe.putString(f.Name); err != nil {
						return err
					}
					pe.putInt16(f.MinVersion)
					pe.putInt16(f.MaxVersion)
					pe.putEmptyTaggedFieldArray()
				}
				return nil
			}
		}
		if r.FinalizedFeaturesEpoch != -1 || len(r.FinalizedFeatures) > 0 {
			epoch := r.FinalizedFeaturesEpoch
			encoders[1] = func(pe packetEncoder) error {
				pe.putInt64(epoch)
				return nil
			}
		}
		if len(r.FinalizedFeatures) > 0 {
			features := r.FinalizedFeatures
			encoders[2] = func(pe packetEncoder) error {
				if err := pe.putArrayLength(len(features)); err != nil {
					return err
				}
				for _, f := range features {
					if err := pe.putString(f.Name); err != nil {
						return err
					}
					pe.putInt16(f.MaxVersionLevel)
					pe.putInt16(f.MinVersionLevel)
					pe.putEmptyTaggedFieldArray()
				}
				return nil
			}
		}
		if err := pe.putTaggedFieldArray(encoders); err != nil {
			return err
		}
	}

	return nil
}

func (r *ApiVersionsResponse) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version
	r.FinalizedFeaturesEpoch = -1
	if r.ErrorCode, err = pd.getInt16(); err != nil {
		return err
	}

	// KIP-511: if broker didn't understand the ApiVersionsRequest version then
	// it replies with a V0 non-flexible ApiVersionResponse where its supported
	// ApiVersionsRequest version is available in ApiKeys
	if r.ErrorCode == int16(ErrUnsupportedVersion) {
		// drop version to 0 and to revert packageDecoder to non-flexible for remaining decoding
		r.Version = 0
		pd = downgradeFlexibleDecoder(pd)
	}

	numApiKeys, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	r.ApiKeys = make([]ApiVersionsResponseKey, numApiKeys)
	for i := 0; i < numApiKeys; i++ {
		var block ApiVersionsResponseKey
		if err = block.decode(pd, r.Version); err != nil {
			return err
		}
		r.ApiKeys[i] = block
	}

	if r.Version >= 1 {
		if r.ThrottleTimeMs, err = pd.getInt32(); err != nil {
			return err
		}
	}

	if r.Version >= 3 {
		err = pd.getTaggedFieldArray(taggedFieldDecoders{
			0: func(pd packetDecoder) error {
				n, err := pd.getArrayLength()
				if err != nil {
					return err
				}
				r.SupportedFeatures = make([]ApiVersionsSupportedFeatureKey, n)
				for i := 0; i < n; i++ {
					name, err := pd.getString()
					if err != nil {
						return err
					}
					minVer, err := pd.getInt16()
					if err != nil {
						return err
					}
					maxVer, err := pd.getInt16()
					if err != nil {
						return err
					}
					if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
						return err
					}
					r.SupportedFeatures[i] = ApiVersionsSupportedFeatureKey{
						Name:       name,
						MinVersion: minVer,
						MaxVersion: maxVer,
					}
				}
				return nil
			},
			1: func(pd packetDecoder) error {
				epoch, err := pd.getInt64()
				if err != nil {
					return err
				}
				r.FinalizedFeaturesEpoch = epoch
				return nil
			},
			2: func(pd packetDecoder) error {
				n, err := pd.getArrayLength()
				if err != nil {
					return err
				}
				r.FinalizedFeatures = make([]ApiVersionsFinalizedFeatureKey, n)
				for i := 0; i < n; i++ {
					name, err := pd.getString()
					if err != nil {
						return err
					}
					maxLvl, err := pd.getInt16()
					if err != nil {
						return err
					}
					minLvl, err := pd.getInt16()
					if err != nil {
						return err
					}
					if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
						return err
					}
					r.FinalizedFeatures[i] = ApiVersionsFinalizedFeatureKey{
						Name:            name,
						MaxVersionLevel: maxLvl,
						MinVersionLevel: minLvl,
					}
				}
				return nil
			},
		})
		return err
	}

	return nil
}

func (r *ApiVersionsResponse) key() int16 {
	return apiKeyApiVersions
}

func (r *ApiVersionsResponse) version() int16 {
	return r.Version
}

func (r *ApiVersionsResponse) headerVersion() int16 {
	// ApiVersionsResponse always includes a v0 header.
	// See KIP-511 for details
	return 0
}

func (r *ApiVersionsResponse) isValidVersion() bool {
	return r.Version >= 0 && r.Version <= 3
}

func (r *ApiVersionsResponse) isFlexible() bool {
	return r.isFlexibleVersion(r.Version)
}

func (r *ApiVersionsResponse) isFlexibleVersion(version int16) bool {
	return version >= 3
}

func (r *ApiVersionsResponse) requiredVersion() KafkaVersion {
	switch r.Version {
	case 3:
		return V2_4_0_0
	case 2:
		return V2_0_0_0
	case 1:
		return V0_11_0_0
	case 0:
		return V0_10_0_0
	default:
		return V2_4_0_0
	}
}

func (r *ApiVersionsResponse) throttleTime() time.Duration {
	return time.Duration(r.ThrottleTimeMs) * time.Millisecond
}
