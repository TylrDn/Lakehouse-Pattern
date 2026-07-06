package lakehouse

# OPA policy that mirrors the Unity Catalog grants in
# ../unity_catalog_setup.sql. If you edit the SQL, edit this file too — CI
# has a smoke test that walks every (principal, action, resource) tuple.

default allow := false

# Admins can do anything.
allow if {
    input.groups[_] == "admins"
}

# Analysts: SELECT on gold only.
allow if {
    input.action == "select"
    startswith(input.resource, "main.gold.")
    input.groups[_] == "analysts"
}

# Data engineers: read/write bronze + silver.
allow if {
    input.action in {"select", "insert", "update", "merge"}
    layer := split(input.resource, ".")[1]
    layer in {"bronze", "silver"}
    input.groups[_] == "data-engineers"
}

# ML engineers: SELECT on silver + gold (they own feature engineering).
allow if {
    input.action == "select"
    layer := split(input.resource, ".")[1]
    layer in {"silver", "gold"}
    input.groups[_] == "ml-engineers"
}
