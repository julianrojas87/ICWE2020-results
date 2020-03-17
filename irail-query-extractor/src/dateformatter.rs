use chrono::{DateTime, Utc};
use serde::{self, Serializer};

const FORMAT: &'static str = "%Y-%m-%dT%H:%M:%S.00Z";

pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where S: Serializer,
{
    let s = format!("{}", date.format(FORMAT));
    serializer.serialize_str(&s)
}
