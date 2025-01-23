
//use self::graph_meta_proto::meta::GraphMeta;
use super::graph_meta_proto::meta::GraphMeta;

use std::sync::Mutex;
use lazy_static::lazy_static;
use std::collections::HashMap;

pub struct GraphMetaCache {
    pub cache: HashMap<u32, GraphMeta>,
}

impl GraphMetaCache {
    fn new() -> GraphMetaCache {
        GraphMetaCache { cache: HashMap::new() }
    }
}

lazy_static! {
    pub static ref GRAPH_META_CACHE: Mutex<GraphMetaCache> = Mutex::new(GraphMetaCache::new());
}
