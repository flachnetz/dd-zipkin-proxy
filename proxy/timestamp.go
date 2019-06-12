package proxy

import "time"

// timestamp in nano seconds
type Timestamp int64

func Microseconds(us int64) Timestamp {
	return Timestamp(us * int64(time.Microsecond))
}

func (ts Timestamp) ToMillis() int64 {
	return int64(ts) / int64(time.Millisecond)
}

func (ts Timestamp) ToMicros() int64 {
	return int64(ts) / int64(time.Microsecond)
}

func (ts Timestamp) ToNanos() int64 {
	return int64(ts)
}

func (ts Timestamp) ToTime() time.Time {
	return time.Unix(0, ts.ToNanos())
}

func (ts Timestamp) Add(duration time.Duration) Timestamp {
	return Timestamp(int64(ts) + int64(duration))
}

func (ts Timestamp) IsValid() bool {
	return ts > 0
}

func (ts *Timestamp) AddInPlace(duration time.Duration) {
	*ts += Timestamp(duration)
}

func (ts Timestamp) MarshalJSON() ([]byte, error) {
	return ts.ToTime().MarshalJSON()
}

func (ts *Timestamp) UnmarshalJSON(encoded []byte) error {
	var timestamp time.Time
	if err := timestamp.UnmarshalJSON(encoded); err != nil {
		return err
	}

	*ts = Timestamp(timestamp.UnixNano())
	return nil
}

