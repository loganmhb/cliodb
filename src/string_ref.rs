
use std::fmt::{self, Display, Debug, Formatter};
use std::iter::FromIterator;
use std::ops::Deref;
use std::borrow::Cow;
use std::sync::Mutex;
use std::collections::HashSet;

#[derive(PartialEq, Eq, Ord, PartialOrd, Clone, Copy, Hash)]
pub struct StringRef(&'static str);

impl Display for StringRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Debug for StringRef {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_tuple("StringRef").field(&self.0).field(&(self.0 as *const _)).finish()
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
            let string: String = val.clone().into_owned();
            map.insert(string);
        }

        let s = map.get(&*val).unwrap();

        // TODO: review saftey
        StringRef(unsafe { ::std::mem::transmute(&**s) })
    }
}
