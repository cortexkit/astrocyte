//! A single hidden dimension keeps attribution independent of report groupings.
use super::{encode, ReportError};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) struct Attribution {
    pub alfonso: Option<String>,
    pub parent_alfonso: Option<String>,
    pub workload: Option<String>,
    pub task: Option<String>,
    pub project_id: Option<String>,
    pub workspace: Option<String>,
}

impl Attribution {
    pub fn dictionary_value(&self) -> Result<Option<String>, ReportError> {
        // The fixed null dictionary row represents Not attributed, avoiding a
        // second spelling for the all-null tuple in persisted summary keys.
        if self == &Self::default() {
            Ok(None)
        } else {
            encode(self).map(Some)
        }
    }

    pub fn from_dictionary(value: Option<String>) -> Result<Self, ReportError> {
        value.map_or_else(
            || Ok(Self::default()),
            |text| {
                serde_json::from_str(&text)
                    .map_err(|_| ReportError::Integrity("invalid attribution tuple"))
            },
        )
    }
}
