package helper

import (
	"fmt"
	"github.com/hdt3213/rdb/model"
	"regexp"
	"slices"
	"time"
)

type decoder interface {
	Parse(cb func(object model.RedisObject) bool) error
}

type regexDecoder struct {
	reg *regexp.Regexp
	dec decoder
}

func (d *regexDecoder) Parse(cb func(object model.RedisObject) bool) error {
	return d.dec.Parse(func(object model.RedisObject) bool {
		if d.reg.MatchString(object.GetKey()) {
			return cb(object)
		}
		return true
	})
}

// regexWrapper returns
func regexWrapper(d decoder, expr string) (*regexDecoder, error) {
	reg, err := regexp.Compile(expr)
	if err != nil {
		return nil, fmt.Errorf("illegal regex expression: %v", expr)
	}
	return &regexDecoder{
		dec: d,
		reg: reg,
	}, nil
}

// RegexOption enable regex filters
type RegexOption *string

// WithRegexOption creates a WithRegexOption from regex expression
func WithRegexOption(expr string) RegexOption {
	return &expr
}

// noExpiredDecoder filter all expired keys
type expireDecoder struct {
	exp string
	dec decoder
}

func (d *expireDecoder) Parse(cb func(object model.RedisObject) bool) error {
	now := time.Now()
	return d.dec.Parse(func(object model.RedisObject) bool {
		expiration := object.GetExpiration()
		if d.exp == "persistent" && expiration == nil { // 永久性KEY
			return cb(object)
		} else if d.exp == "volatile" && expiration != nil { // 非永久性KEY，易失性
			return cb(object)
		} else if d.exp == "not-expired" && (expiration == nil || expiration.After(now)) { // 未过期的KEY
			return cb(object)
		} else if d.exp == "expired" && expiration.Before(now) { // 已过期的KEY
			return cb(object)
		}
		return true
	})
}

// expireWrapper returns
func expireWrapper(d decoder, expire string) (*expireDecoder, error) {
	options := []string{"persistent", "volatile", "not-expired", "expired"}
	if !slices.Contains(options, expire) {
		return nil, fmt.Errorf("unsupported expire option: %s", expire)
	}
	return &expireDecoder{
		dec: d,
		exp: expire,
	}, nil
}

// ExpireOption tells decoder to filter persistent/volatile/not-expired keys
type ExpireOption *string

// WithExpireOption tells decoder to filter persistent/volatile/not-expired keys
func WithExpireOption(expire string) ExpireOption {
	return &expire
}

func wrapDecoder(dec decoder, options ...interface{}) (decoder, error) {
	var regexOpt RegexOption
	var expireOpt ExpireOption
	for _, opt := range options {
		switch o := opt.(type) {
		case RegexOption:
			regexOpt = o
		case ExpireOption:
			expireOpt = o
		}
	}
	if regexOpt != nil {
		var err error
		dec, err = regexWrapper(dec, *regexOpt)
		if err != nil {
			return nil, err
		}
	}
	if expireOpt != nil {
		var err error
		dec, err = expireWrapper(dec, *expireOpt)
		if err != nil {
			return nil, err
		}
	}
	return dec, nil
}
