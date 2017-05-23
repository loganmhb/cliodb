use std::collections::HashMap;
use datalog::Value;

struct QueryContext {
    // Map of symbol to tuple positions
    symbols: HashMap<String, u64>,
    // The tuples themselves
    tuples: Vec<Value>
}
