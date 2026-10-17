package consensus

import (
	"bytes"
	"errors"
	"testing"
)

func TestPutThenGet(t *testing.T) {
	s := New()
	if r := s.Apply(Command{Kind: KindPut, Value: []byte("hello")}); r.Err != nil {
		t.Fatalf("put: %v", r.Err)
	}
	r := s.Apply(Command{Kind: KindGet})
	if r.Err != nil {
		t.Fatalf("get: %v", r.Err)
	}
	if !r.Exists || !bytes.Equal(r.Value, []byte("hello")) {
		t.Fatalf("unexpected get: %+v", r)
	}
}

func TestPutAlreadyExists(t *testing.T) {
	s := New()
	_ = s.Apply(Command{Kind: KindPut, Value: []byte("x")})
	r := s.Apply(Command{Kind: KindPut, Value: []byte("y")})
	if !errors.Is(r.Err, ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", r.Err)
	}
}

func TestUpdateNotFound(t *testing.T) {
	s := New()
	r := s.Apply(Command{Kind: KindUpdate, Value: []byte("x")})
	if !errors.Is(r.Err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", r.Err)
	}
}

func TestUpdateOverwrites(t *testing.T) {
	s := New()
	_ = s.Apply(Command{Kind: KindPut, Value: []byte("old")})
	if r := s.Apply(Command{Kind: KindUpdate, Value: []byte("new")}); r.Err != nil {
		t.Fatalf("update: %v", r.Err)
	}
	r := s.Apply(Command{Kind: KindGet})
	if !bytes.Equal(r.Value, []byte("new")) {
		t.Fatalf("got %q, want %q", r.Value, "new")
	}
}

func TestGetMissing(t *testing.T) {
	s := New()
	r := s.Apply(Command{Kind: KindGet})
	if r.Err != nil || r.Exists {
		t.Fatalf("want empty get, got %+v", r)
	}
}

func TestApplyReturnsCopy(t *testing.T) {
	s := New()
	_ = s.Apply(Command{Kind: KindPut, Value: []byte("abc")})
	r := s.Apply(Command{Kind: KindGet})
	r.Value[0] = 'Z'
	r2 := s.Apply(Command{Kind: KindGet})
	if bytes.Equal(r.Value, r2.Value) {
		t.Fatalf("apply returned shared slice; mutation leaked back into state")
	}
}

func TestUnknownKind(t *testing.T) {
	s := New()
	r := s.Apply(Command{Kind: 99})
	if !errors.Is(r.Err, ErrUnknownKind) {
		t.Fatalf("want ErrUnknownKind, got %v", r.Err)
	}
}
