
use std::fmt::{self, Display, Debug, Formatter};
use std::iter::FromIterator;
use std::ops::Deref;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Mutex;
use std::mem;

#[derive(PartialEq, Eq, Ord, PartialOrd, Clone, Copy, Hash)]
pub struct StringRef(&'static str);

impl StringRef {
    pub fn address(&self) -> *const () {
        self.0 as *const str as *const _
    }
}

impl Display for StringRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for StringRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl FromIterator<char> for StringRef {
    fn from_iter<T: IntoIterator<Item = char>>(iter: T) -> Self {
        let string: String = iter.into_iter().collect();
        Self::from(string)
    }
}

impl Deref for StringRef {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, T> From<T> for StringRef
    where T: Into<Cow<'a, str>>
{
    fn from(other: T) -> Self {
        lazy_static! {
            static ref MAP: Mutex<HashSet<String>> = Default::default();
        }

        let val = other.into();
        let mut map = MAP.lock().unwrap();

        if !map.contains(&*val) {
            map.insert(val.clone().into_owned());
        }

        StringRef(unsafe { mem::transmute(&**map.get(&*val).unwrap()) })
    }
}

#[cfg(test)]
mod tests {
    extern crate test;
    use self::test::{Bencher, black_box};
    use std::str;

    use super::*;

    #[test]
    fn same_address() {
        let a = StringRef::from(String::from("Hello"));
        let b = StringRef::from(String::from("Hello"));

        assert_eq!(a.address(), b.address());
    }

    #[bench]
    fn bench_string_ref(b: &mut Bencher) {
        let mut n = 0usize;
        let mut data = [b'0'; 8];

        b.iter(|| {
                   for (i, byte) in data.iter_mut().enumerate() {
                       *byte = (*byte - b'0').wrapping_add((n & i) as u8) % 32 + b'0';
                   }
                   StringRef::from(black_box(unsafe { str::from_utf8_unchecked(&data) }));
                   n += 1;
               });
    }
}
