use super::KvsEngine;
use crate::err::KvsError;

#[allow(dead_code)]
#[derive(Clone)]
pub struct SledEngine {
    db: sled::Db,
}

impl SledEngine {
    const LOG_LOCATION: &str = "sled-logs";

    pub fn open<T: AsRef<std::path::Path>>(t: T) -> crate::Result<SledEngine> {
        let path = t.as_ref();
        path.to_path_buf().push(Self::LOG_LOCATION);

        let db = sled::open(path)?;

        Ok(SledEngine { db })
    }
}

impl KvsEngine for SledEngine {
    fn get(&self, key: String) -> crate::Result<Option<String>> {
        let res = self
            .db
            .get(key)
            .map_err(Into::<crate::err::KvsError>::into)?;
        match res {
            Some(v) => Ok(Some(String::from_utf8(v.to_vec())?)),
            None => Ok(None),
        }
    }

    fn remove(&self, key: String) -> crate::Result<()> {
        let old = self.db.remove(key)?;
        match old {
            Some(_) => {
                self.db.flush()?;
                Ok(())
            }
            None => Err(KvsError::KeyNotFound),
        }
    }

    fn set(&self, key: String, value: String) -> crate::Result<()> {
        self.db
            .insert(key, value.as_bytes())
            .map(|_| ())
            .map_err(Into::<crate::err::KvsError>::into)?;
        self.db.flush()?;
        Ok(())
    }
}
