// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::cache_layer::medium::CacheMedium;
use datafusion::datasource::object_store::ObjectStoreUrl;
use object_store::local::LocalFileSystem;
use object_store::path::{Path, DELIMITER};
use object_store::ObjectStore;
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LocalDiskMedium {
    cache_object_store: Arc<LocalFileSystem>,
    root_cache_dir: Path,
}

impl LocalDiskMedium {
    pub fn new(root_cache_dir: String) -> Self {
        Self {
            cache_object_store: Arc::new(LocalFileSystem::new()),
            root_cache_dir: Path::from(root_cache_dir),
        }
    }
}

impl Display for LocalDiskMedium {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache medium with local disk({})", self.root_cache_dir)
    }
}

impl CacheMedium for LocalDiskMedium {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_object_store(&self) -> Arc<dyn ObjectStore> {
        self.cache_object_store.clone()
    }

    fn get_object_store_url(&self) -> ObjectStoreUrl {
        ObjectStoreUrl::local_filesystem()
    }

    // Later it's better to add as_any() to the ObjectStore
    // and to create a directory for each ObjectStore.
    fn get_mapping_location(
        &self,
        source_location: &Path,
        _source_object_store: Arc<dyn ObjectStore>,
    ) -> Path {
        let cache_type = "local";
        // let cache_type = source_object_store.as_ref().as_any().type_id();
        let cache_location = format!(
            "{}{DELIMITER}{cache_type}{DELIMITER}{source_location}",
            self.root_cache_dir
        );
        Path::from(cache_location)
    }
}
