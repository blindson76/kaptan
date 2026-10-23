package mongo

import "testing"

func TestIsMongoStuck(t *testing.T) {
	if !isMongoStuck(`{"mongo":{"state":3,"stateStr":"RECOVERING","stuck":true,"stuckFor":"1m0s"}}`) {
		t.Fatal("expected stuck")
	}
	if isMongoStuck(`{"mongo":{"state":3,"stateStr":"RECOVERING"}}`) || isMongoStuck("") || isMongoStuck("x") {
		t.Fatal("unexpected stuck")
	}
	if s, str, ok := parseMongoState(`{"mongo":{"state":3,"stateStr":"RECOVERING","stuck":true}}`); !ok || s != 3 || str != "RECOVERING" {
		t.Fatalf("parse: %d %s %v", s, str, ok)
	}
}
