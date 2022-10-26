// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::Range;

use futures::executor::block_on;

use coprocessor_plugin_api::*;

use log::info;

mod graph_meta_proto;
use graph_meta_proto::meta::GraphMeta;
use graph_meta_proto::coprocessor::*;
use protobuf::{Message, CodedOutputStream, CodedInputStream, Enum};

mod graph_meta_cache;
use graph_meta_cache::GRAPH_META_CACHE;

#[derive(Default)]
struct GraphMetaPlugin;

fn encode_u64(value: &u64) -> Vec<u8> {
    let mut ret: Vec<u8> = Vec::with_capacity(8);
    ret.push((value & 0xff) as u8);
    ret.push(((value >> 8) & 0xff) as u8);
    ret.push(((value >> 16) & 0xff) as u8);
    ret.push(((value >> 24) & 0xff) as u8);
    ret.push(((value >> 32) & 0xff) as u8);
    ret.push(((value >> 40) & 0xff) as u8);
    ret.push(((value >> 48) & 0xff) as u8);
    ret.push(((value >> 56) & 0xff) as u8);
    ret
}

impl CoprocessorPlugin for GraphMetaPlugin {
    fn on_raw_coprocessor_request(
        &self,
        _ranges: Vec<Range<Key>>,
        _request: RawRequest,
        _storage: &dyn RawStorage,
    ) -> PluginResult<RawResponse> {
        let request_type = _request.get(0).unwrap();
        let request_len = _request.len();

        let mut is = CodedInputStream::from_bytes(&_request);

        // Read request type
        let request_type_num = is.read_fixed32().unwrap() as i32;
        let request_type = RequestType::from_i32(request_type_num).unwrap();
    
        // Handle request
        match request_type {
            RequestType::UPDATE_GRAPH_META => {
                // Parse
                let update_meta_request = UpdateGraphMetaRequest::parse_from(&mut is).unwrap();
                let graph_metas = update_meta_request.graphMetas;

                for graph_meta in graph_metas {
                    // Insert to cache
                    let graph_id = graph_meta.id;
                    let mut graph_meta_cache = GRAPH_META_CACHE.lock().unwrap();
                    graph_meta_cache.cache.insert(graph_id, graph_meta);
                }

                // Generate response
                let result_bytes = UpdateGraphMetaResponse::default().write_to_bytes().unwrap();
                return PluginResult::Ok(result_bytes);
            }

            // Just for test
            RequestType::GET_GRAPH_META => {
                // Parse
                let get_meta_request = GetGraphMetaRequest::parse_from(&mut is).unwrap();
                let graph_id = get_meta_request.graphId;

                // Get meta data
                let graph_meta_cache = GRAPH_META_CACHE.lock().unwrap();
                let graph_meta = graph_meta_cache.cache.get(&graph_id).unwrap();

                // Generate response
                let result_bytes = graph_meta.write_to_bytes().unwrap();
                return PluginResult::Ok(result_bytes);
            }

            RequestType::COUNT => {
                // Parse
                let count_request = CountRequest::parse_from(&mut is).unwrap();

                // Count
                let mut counter:i64 = 0;
                for range in &_ranges {
                    let scan_result_future = _storage.scan(range.clone());
                    let scan_result = block_on(scan_result_future);
                    let data_rows = scan_result.unwrap();
                    println!("scan result {} ", data_rows.len());
                    counter += data_rows.len() as i64;
                }
        
                println!("Total scan result is {}", counter);

                // Generate response
                let mut count_response = CountResponse::new();
                count_response.counter = counter;
                let serialize_ret = count_response.write_to_bytes().unwrap();
                return PluginResult::Ok(serialize_ret);
            }
        }
        
        /*
        match request_type {
            0 => {
                // Update graph meta data
                let request_data = &_request[1..request_len];
                // Parse
                let graph_meta = graph_meta_proto::meta::GraphMeta::parse_from_bytes(request_data).unwrap();

                // Insert to cache
                let graph_id = graph_meta.id;
                let mut graph_meta_cache = GRAPH_META_CACHE.lock().unwrap();
                graph_meta_cache.cache.insert(graph_id, graph_meta);

                // Just for test
                let result_bytes = graph_meta_cache.cache.get(&graph_id).unwrap().write_to_bytes().unwrap();
                return PluginResult::Ok(result_bytes);
            },
            1 => {
                // Count
                let mut counter:u64 = 0;
                for range in &_ranges {
                    let scan_result_future = _storage.scan(range.clone());
                    let scan_result = block_on(scan_result_future);
                    let data_rows = scan_result.unwrap();
                    println!("scan result {} ", data_rows.len());
                    counter += data_rows.len() as u64;
                }
        
                println!("Total scan result is {}", counter);
                let serialize_ret = encode_u64(&counter);
                return PluginResult::Ok(serialize_ret);
            }, 

            2 => {
                // Get graph meta data
                let graph_id = 0;
                let mut graph_meta_cache = GRAPH_META_CACHE.lock().unwrap();

                // Just for test
                let result_bytes = graph_meta_cache.cache.get(&graph_id).unwrap().write_to_bytes().unwrap();
                return PluginResult::Ok(result_bytes);
            }

            3 => {
                // Parse key/value
                

                for range in &_ranges {
                    let scan_result_future = _storage.scan(range.clone());
                    let scan_result = block_on(scan_result_future);
                    let data_rows = scan_result.unwrap();
                    
                    for data_row in data_rows {
                        let mut is = CodedInputStream::from_bytes(data_row);
                    }

                    println!("scan result {} ", data_rows.len());
                    counter += data_rows.len() as u64;
                }
        
            }

            _ => PluginResult::Err(PluginError::Other(String::from("Not support retquest type new"), Box::new(1))),
        }
        */
    }
}

declare_plugin!(GraphMetaPlugin::default());
