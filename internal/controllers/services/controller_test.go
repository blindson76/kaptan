package services

import "testing"

func TestBuildPlacementPlanKeepsRolesAndStopsExtras(t *testing.T) {
	plan := buildPlacementPlan([]serviceInstance{
		{NodeID: "node-a", Role: serviceRoleMaster},
		{NodeID: "node-b", Role: serviceRoleSlave},
		{NodeID: "node-c", Role: serviceRoleSlave},
	}, desiredRoles(2))

	if len(plan.MissingRoles) != 0 {
		t.Fatalf("expected no missing roles, got %v", plan.MissingRoles)
	}
	if !plan.UsedNodes["node-a"] || !plan.UsedNodes["node-b"] {
		t.Fatalf("expected master/slave nodes to be kept, got %#v", plan.UsedNodes)
	}
	if len(plan.ExtraNodes) != 1 || plan.ExtraNodes[0] != "node-c" {
		t.Fatalf("expected node-c to be stopped, got %v", plan.ExtraNodes)
	}
}

func TestBuildPlacementPlanRestoresMissingMaster(t *testing.T) {
	plan := buildPlacementPlan([]serviceInstance{
		{NodeID: "node-b", Role: serviceRoleSlave},
	}, desiredRoles(2))

	if len(plan.MissingRoles) != 1 || plan.MissingRoles[0] != serviceRoleMaster {
		t.Fatalf("expected missing master, got %v", plan.MissingRoles)
	}
	if !plan.UsedNodes["node-b"] {
		t.Fatalf("expected existing slave node to stay active, got %#v", plan.UsedNodes)
	}
	if len(plan.ExtraNodes) != 0 {
		t.Fatalf("expected no extra nodes, got %v", plan.ExtraNodes)
	}
}

func TestPickPlacementNodePrefersIdleNodeBeforeReusingExtraNode(t *testing.T) {
	id, ok := pickPlacementNode(
		[]string{"node-a", "node-b", "node-c"},
		map[string]bool{"node-a": true},
		map[string]bool{"node-a": true, "node-b": true},
		map[string]bool{},
		0,
	)
	if !ok {
		t.Fatal("expected a placement node")
	}
	if id != "node-c" {
		t.Fatalf("expected idle node-c to be preferred, got %s", id)
	}
}
