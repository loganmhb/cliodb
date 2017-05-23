pub mod ast;

// # Initial pass
// A database is just a log of facts. Facts are (entity, attribute, value) triples.
// Attributes and values are both just strings. There are no transactions or histories.
trait Database {
    fn add(&mut self, fact: Fact);
}

#[derive(Debug, PartialEq)]
struct Fact {
    entity: u64,
    attribute: String,
    value: String
}

impl Fact {
    pub fn new(e: u64, a: &str, v: &str) -> Fact {
        Fact {entity: e, attribute: a.to_owned(), value: v.to_owned()}
    }
}

#[derive(Debug)]
struct InMemoryLog {
    facts: Vec<Fact>
}

impl InMemoryLog {
    pub fn new() -> InMemoryLog {
        InMemoryLog { facts: Vec::new() }
    }
}

impl IntoIterator for InMemoryLog {
    type Item = Fact;
    type IntoIter = ::std::vec::IntoIter<Fact>;

    fn into_iter(self) -> Self::IntoIter {
        self.facts.into_iter()
    }
}

impl Database for InMemoryLog {
    fn add(&mut self, fact: Fact) {
        self.facts.push(fact);
    }
}

#[cfg(test)]
mod test {
    use datalog::{Fact, InMemoryLog, Database};

    #[test]
    fn test_insertion() {
        let fact = Fact::new(0, "name", "Bob");
        let mut db = InMemoryLog::new();
        db.add(fact);
        let inserted = db.into_iter().take(1).nth(0).unwrap();
        assert!(inserted.entity == 0);
        assert!(inserted.attribute == "name");
        assert!(inserted.value == "Bob");
    }
}
