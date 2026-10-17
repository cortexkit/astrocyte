//! Run the build script's provenance tests under Cargo's normal test target.

#[allow(dead_code)] // Cargo owns the included script's main; this target owns its tests.
#[path = "../build.rs"]
mod build_script;
